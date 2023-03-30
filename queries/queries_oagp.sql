-- 5, 10, 15, 20, 25, 30, 35, 40, 45, 50

SELECT DEDUP * FROM oag.publications WHERE MOD(id, 40) < 1
SELECT DEDUP * FROM oag.publications WHERE MOD(id, 20) < 1
-- SELECT DEDUP * FROM oag.people500k WHERE MOD(index, 10) < 1
-- SELECT DEDUP * FROM oag.people500k WHERE MOD(index, 5) < 1
-- SELECT DEDUP * FROM oag.people500k WHERE MOD(index, 2.5) < 1

-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 20) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 10) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 6.6) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 5) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 4) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 3.3) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 2.85) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 2.5) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 2.22) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 2) < 1
--
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 20) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 10) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 6.6) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 5) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 4) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 3.3) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 2.85) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 2.5) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 2.22) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 2) < 1

-- Overlapping 2.5  5 (2.5), 10 (5), 20 (10), 40 (20),
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 40) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 20) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 10) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 5) < 1
-- SELECT DEDUP * FROM oag.papers200k WHERE MOD(index, 2.5) < 1
--
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 40) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 20) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 10) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 5) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 2.5) < 1
--
--
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 2) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 1.5) < 1
-- SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 1.2) < 1


--AND doc_type = 'Journal'
-- 0.5, 1 (0.5), 2.5 (1), 5 (2.5), 10 (5), 20 (10), 40 (20)
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 2015
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 2014 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 2013 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 2011 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 2006 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 1990 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers200k WHERE year > 2010
--
-- SELECT DEDUP * FROM oag.papers1m WHERE year > 2013 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers1m WHERE year > 2011 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers1m WHERE year > 2006 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers1m WHERE year > 1990 AND CAST(n_citation AS DOUBLE) > 0
-- SELECT DEDUP * FROM oag.papers1m WHERE year > 2010

-- SELECT DEDUP * FROM oag.papers2m WHERE year > 2015