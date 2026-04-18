"""Pytest defaults: tránh gọi mạng nặng khi không chỉ định."""

from __future__ import annotations

import os

# BCTC nhiều kỳ (VNDirect) — bật tay trong môi trường thật: II_FETCH_FINANCIAL_STATEMENTS=1
os.environ.setdefault("II_FETCH_FINANCIAL_STATEMENTS", "0")
os.environ.setdefault("II_SKIP_PEER_FETCH", "1")
os.environ.setdefault("II_SNAPSHOT_DISK_CACHE", "0")
