# coding: utf-8
from __future__ import division, print_function, unicode_literals

import re
import threading
import time

from .__init__ import unicode
from .util import UNHUMANIZE_UNITS

if True:  # pylint: disable=using-constant-test
    from typing import Any, Optional

try:
    from time import monotonic
except ImportError:
    monotonic = time.time

RE_RATE = re.compile(r"^([0-9]+(?:\.[0-9]+)?) *([kmgt]?)i?b?(?:/s)?$")

# smallest write while throttled; keeps syscall overhead sane at very low caps
MIN_BUFSZ = 1024


def parse_rate(zs: str) -> int:
    zs = unicode(zs).strip().lower()
    if zs in ("", "no"):
        return 0

    m = RE_RATE.match(zs)
    if not m:
        raise Exception("invalid speed [%s]; examples: 512k, 4.5m, 1g" % (zs,))

    return int(float(m.group(1)) * UNHUMANIZE_UNITS.get(m.group(2), 1))


def load_bwlim(args: Any) -> None:
    args.dl_bw_all = parse_rate(args.dl_bw_all)
    args.dl_bw_usr = parse_rate(args.dl_bw_usr)
    ret: dict[str, int] = {}
    for zs in args.dl_bw or []:
        for zs2 in zs.split(","):
            zs2 = zs2.strip()
            if not zs2:
                continue
            k, sep, v = zs2.rpartition("=")
            k = k.strip()
            if not sep or not k:
                raise Exception("invalid --dl-bw value [%s]; need USER=SPEED" % (zs2,))
            ret[k] = parse_rate(v)
    args.dl_bw = ret


class _Bucket(object):
    __slots__ = ("rate", "t_next", "nstreams")

    def __init__(self, rate: int) -> None:
        self.rate = rate
        self.t_next = 0.0
        self.nstreams = 0


class BwLim(object):
    def __init__(self, args: Any) -> None:
        self.mutex = threading.Lock()
        self.dfl = args.dl_bw_usr
        self.ovr: dict[str, int] = args.dl_bw
        self.grp_ovr = any(k.startswith("@") for k in self.ovr)
        self.maxsz = max(MIN_BUFSZ, args.s_wr_sz)
        self.glob = _Bucket(args.dl_bw_all) if args.dl_bw_all else None
        self.usrs: dict[tuple[str, str], _Bucket] = {}

    def user_rate(self, asrv: Any, uname: str) -> int:
        """0 = no per-user cap"""
        ovr = self.ovr
        if uname in ovr:
            return ovr[uname]

        if self.grp_ovr and uname != "*":
            gns = list(asrv.idp_accs.get(uname) or [])
            gns += [gn for gn, uns in asrv.grps.items() if uname in uns]
            rates = [ovr["@" + gn] for gn in gns if "@" + gn in ovr]
            if rates:
                # the most generous group wins, and 0 (uncapped) beats any number
                return 0 if 0 in rates else max(rates)

        return self.dfl

    def open(self, asrv: Any, uname: str, ip: str) -> Optional["BwStream"]:
        rate = self.user_rate(asrv, uname)
        if not rate and not self.glob:
            return None

        # anonymous visitors all share the name "*", so cap them per IP instead
        key = (uname, ip if uname == "*" else "")
        buckets = []
        with self.mutex:
            if rate:
                ub = self.usrs.get(key)
                if not ub:
                    if len(self.usrs) > 256:
                        self._gc()
                    ub = self.usrs[key] = _Bucket(rate)
                ub.rate = rate
                buckets.append(ub)
            if self.glob:
                buckets.append(self.glob)
            for b in buckets:
                b.nstreams += 1

        return BwStream(self, key if rate else None, buckets)

    def _gc(self) -> None:
        # an idle bucket is kept until its last reservation has elapsed, so
        # back-to-back small downloads cannot each start with a fresh bucket
        now = monotonic()
        for k, b in list(self.usrs.items()):
            if b.nstreams <= 0 and b.t_next <= now:
                del self.usrs[k]


class BwStream(object):
    def __init__(
        self, lim: BwLim, key: Optional[tuple[str, str]], buckets: list[_Bucket]
    ) -> None:
        self.lim = lim
        self.key = key
        self.buckets = buckets
        self.bufsz = lim.maxsz
        self.closed = False
        self.ends = [0.0] * len(buckets)
        self._resize()

    def _resize(self) -> None:
        share = min(b.rate / max(1, b.nstreams) for b in self.buckets)
        # an eighth of a second per write keeps the stream smooth
        self.bufsz = max(MIN_BUFSZ, min(self.lim.maxsz, int(share / 8)))

    def take(self, nbytes: int) -> None:
        # queue at the tail of each bucket and wait for the slot to start;
        # concurrent streams then interleave and split each bucket's rate
        with self.lim.mutex:
            now = monotonic()
            t = now
            for n, b in enumerate(self.buckets):
                t0 = b.t_next if b.t_next > now else now
                b.t_next = self.ends[n] = t0 + nbytes / b.rate
                if t < t0:
                    t = t0
            self._resize()

        if t > now:
            time.sleep(t - now)

    def give(self, nbytes: int) -> None:
        """return the unsent part of the latest reservation"""
        if not nbytes:
            return
        with self.lim.mutex:
            for n, b in enumerate(self.buckets):
                # rewinding below someone else's later slot would double-book it
                if b.t_next == self.ends[n]:
                    b.t_next -= nbytes / b.rate

    def sendall(self, sck: Any, buf: bytes) -> None:
        ofs = 0
        while ofs < len(buf):
            zb = buf[ofs : ofs + self.bufsz]
            self.take(len(zb))
            sck.sendall(zb)
            ofs += len(zb)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        lim = self.lim
        with lim.mutex:
            for b in self.buckets:
                b.nstreams -= 1
            if self.key:
                ub = lim.usrs.get(self.key)
                if ub and ub.nstreams <= 0 and ub.t_next <= monotonic():
                    del lim.usrs[self.key]
