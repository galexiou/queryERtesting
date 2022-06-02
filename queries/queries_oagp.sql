-- 5, 10, 15, 20, 25, 30, 35, 40, 45, 50
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 20) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 10) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 6.6) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 5) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 4) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 3.3) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 2.85) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 2.5) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 2.22) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 2) < 1

SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 20) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 10) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 6.6) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 5) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 4) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 3.3) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 2.85) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 2.5) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 2.22) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 2) < 1

-- Overlapping 5, 15, 25, 35, 45