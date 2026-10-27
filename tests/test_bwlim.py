#!/usr/bin/env python3
# coding: utf-8
from __future__ import division, print_function, unicode_literals

import threading
import time
import unittest
from argparse import Namespace

from copyparty.bwlim import BwLim, load_bwlim, parse_rate


class VSock(object):
    def __init__(self):
        self.n = 0

    def sendall(self, buf):
        self.n += len(buf)


class VAsrv(object):
    def __init__(self, grps=None, idp_accs=None):
        self.grps = grps or {}
        self.idp_accs = idp_accs or {}


def mkargs(glob="0", usr="0", ovr=None):
    args = Namespace(dl_bw_all=glob, dl_bw_usr=usr, dl_bw=ovr, s_wr_sz=256 * 1024)
    load_bwlim(args)
    return args


class TestBwLim(unittest.TestCase):
    def test_parse_rate(self):
        for zs, n in [
            ("0", 0),
            ("no", 0),
            ("", 0),
            ("100", 100),
            ("512k", 512 * 1024),
            ("4.5m", int(4.5 * 1024 * 1024)),
            ("1G", 1024 * 1024 * 1024),
            ("5MiB/s", 5 * 1024 * 1024),
            ("5 mb", 5 * 1024 * 1024),
        ]:
            self.assertEqual(parse_rate(zs), n, zs)

        for zs in ["5x", "m", "-1", "1.2.3k"]:
            self.assertRaises(Exception, parse_rate, zs)

    def test_load(self):
        args = mkargs("6m", "2m", ["alice=4m, @fast=0", "*=1k"])
        self.assertEqual(args.dl_bw_all, 6 * 1024 * 1024)
        self.assertEqual(args.dl_bw_usr, 2 * 1024 * 1024)
        self.assertEqual(args.dl_bw, {"alice": 4 << 20, "@fast": 0, "*": 1024})
        self.assertRaises(Exception, mkargs, "0", "0", ["alice"])

    def test_user_rate(self):
        args = mkargs("0", "2m", ["dave=7m, @fast=0, @slow=1m, @mid=3m, *=1k"])
        asrv = VAsrv(
            {"fast": ["carol"], "slow": ["carol", "dave", "fred"], "mid": ["fred"]},
            {"erin": ["fast"]},
        )
        lim = BwLim(args)
        M = 1024 * 1024
        for un, n in [
            ("alice", 2 * M),  # default
            ("carol", 0),  # uncapped group beats a capped one
            ("dave", 7 * M),  # explicit user beats groups
            ("erin", 0),  # idp group
            ("fred", 3 * M),  # fastest group wins
            ("*", 1024),
        ]:
            self.assertEqual(lim.user_rate(asrv, un), n, un)

        # uncapped user and no global cap = no limiter at all
        self.assertIsNone(lim.open(asrv, "carol", "1.2.3.4"))

    def run_streams(self, lim, users, dur=2.0):
        asrv = VAsrv()
        socks = [VSock() for _ in users]
        streams = [lim.open(asrv, un, ip) for un, ip in users]
        t_end = time.time() + dur

        def worker(bwl, sck):
            while time.time() < t_end:
                bwl.sendall(sck, b"\x00" * bwl.bufsz)

        thrs = [
            threading.Thread(target=worker, args=(bwl, sck))
            for bwl, sck in zip(streams, socks)
        ]
        for t in thrs:
            t.start()
        for t in thrs:
            t.join()
        for bwl in streams:
            bwl.close()

        return [x.n / dur for x in socks]

    def assertNear(self, got, want):
        # one write of slack in either direction; each write is 1/8 sec of rate
        self.assertTrue(abs(got - want) < want * 0.25, "%d != %d" % (got, want))

    def test_user_share(self):
        R = 64 * 1024
        lim = BwLim(mkargs("0", "64k"))
        users = [("alice", ""), ("alice", ""), ("bob", ""), ("*", "1.1.1.1")]
        a1, a2, b, anon = self.run_streams(lim, users)
        self.assertNear(a1, R / 2)
        self.assertNear(a2, R / 2)
        self.assertNear(b, R)
        self.assertNear(anon, R)
        self.assertNear(a1 + a2, R)

    def test_anon_per_ip(self):
        R = 64 * 1024
        lim = BwLim(mkargs("0", "64k"))
        users = [("*", "1.1.1.1"), ("*", "1.1.1.1"), ("*", "2.2.2.2")]
        a1, a2, b = self.run_streams(lim, users)
        self.assertNear(a1 + a2, R)
        self.assertNear(b, R)

    def test_global(self):
        R = 96 * 1024
        lim = BwLim(mkargs("96k", "0", ["bob=16k"]))
        users = [("alice", ""), ("alice", ""), ("bob", "")]
        a1, a2, b = self.run_streams(lim, users)
        self.assertNear(b, 16 * 1024)
        # bob is held back by his own cap, so alice gets what bob leaves over
        self.assertNear(a1 + a2, R - 16 * 1024)
        self.assertNear(a1 + a2 + b, R)

    def test_debt_outlives_stream(self):
        lim = BwLim(mkargs("0", "1k"))
        bwl = lim.open(VAsrv(), "alice", "")
        bwl.take(2048)
        bwl.close()
        # the bucket remains until its reservation has elapsed, so a new
        # download cannot skip the wait by reconnecting
        self.assertIn(("alice", ""), lim.usrs)
        self.assertEqual(lim.usrs[("alice", "")].nstreams, 0)

        bwl = lim.open(VAsrv(), "alice", "")
        t0 = time.time()
        bwl.take(1)
        self.assertGreater(time.time() - t0, 1.5)
        bwl.close()

    def test_give(self):
        lim = BwLim(mkargs("0", "64k"))
        a = lim.open(VAsrv(), "alice", "")
        b = lim.open(VAsrv(), "alice", "")
        ub = lim.usrs[("alice", "")]

        a.take(1024)
        t_next = ub.t_next
        a.give(1024)
        self.assertLess(ub.t_next, t_next)

        a.take(1024)
        b.take(1024)
        t_next = ub.t_next
        # b already queued behind a, so a cannot hand its slot back
        a.give(1024)
        self.assertEqual(ub.t_next, t_next)
        a.close()
        b.close()
