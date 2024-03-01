WITH RECURSIVE post_all(psa_threadid
                      , psa_thread_creatorid
                      , psa_messageid
                      , psa_creationdate
                      , psa_messagetype
                       ) AS (
    SELECT m_messageid AS psa_threadid
         , m_creatorid AS psa_thread_creatorid
         , m_messageid AS psa_messageid
         , m_creationdate
         , 'Post'
      FROM message
     WHERE 1=1
       AND m_c_replyof IS NULL
       AND m_creationdate BETWEEN '2012-06-01T00:00:00' AND '2012-07-01T00:00:00'
  UNION ALL
    SELECT psa.psa_threadid AS psa_threadid
         , psa.psa_thread_creatorid AS psa_thread_creatorid
         , m_messageid
         , m_creationdate
         , 'Comment'
      FROM message p
         , post_all psa
     WHERE 1=1
       AND p.m_c_replyof = psa.psa_messageid
       AND m_creationdate BETWEEN '2012-06-01T00:00:00' AND '2012-07-01T00:00:00'
)
SELECT p.p_personid AS "person.id"
     , p.p_firstname AS "person.firstName"
     , p.p_lastname AS "person.lastName"
     , count(DISTINCT psa.psa_threadid) AS threadCount
     , count(DISTINCT psa.psa_messageid) AS messageCount
  FROM person p left join post_all psa on (
       1=1
   AND p.p_personid = psa.psa_thread_creatorid
   AND psa_creationdate BETWEEN '2012-06-01T00:00:00' AND '2012-07-01T00:00:00'
   )
 GROUP BY p.p_personid, p.p_firstname, p.p_lastname
 ORDER BY messageCount DESC, p.p_personid
 LIMIT 100
;
