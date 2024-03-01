WITH zombies AS (
    SELECT p.p_personid AS zombieid
      FROM place co 
         , place ci 
         , person p
           LEFT JOIN message m ON (p.p_personid = m.m_creatorid AND m.m_creationdate BETWEEN p.p_creationdate AND '2013-01-01T00:00:00')
     WHERE 1=1
       AND co.pl_placeid = ci.pl_containerplaceid
       AND ci.pl_placeid = p.p_placeid
       AND co.pl_name = 'Belarus'
       AND p.p_creationdate < '2013-01-01T00:00:00'
     GROUP BY p.p_personid, p.p_creationdate
    HAVING count(m_messageid) < 12*extract(YEAR FROM '2013-01-01T00:00:00'::date)+extract(MONTH FROM '2013-01-01T00:00:00'::date) - (12*extract(YEAR FROM p.p_creationdate) + extract(MONTH FROM p.p_creationdate)) + 1
)
SELECT z.zombieid AS "zombie.id"
     , count(zl.zombieid) AS zombieLikeCount
     , count(l.l_personid) AS totalLikeCount
     , CASE WHEN count(l.l_personid) = 0 THEN 0 ELSE count(zl.zombieid)::float/count(l.l_personid) END AS zombieScore
  FROM message m
       INNER JOIN likes l ON (m.m_messageid = l.l_messageid)
       INNER JOIN person p ON (l.l_personid = p.p_personid AND p.p_creationdate < '2013-01-01T00:00:00')
       LEFT  JOIN zombies zl ON (p.p_personid = zl.zombieid)
       RIGHT JOIN zombies z ON (z.zombieid = m.m_creatorid)
 GROUP BY z.zombieid
 ORDER BY zombieScore DESC, z.zombieid
 LIMIT 100
;
