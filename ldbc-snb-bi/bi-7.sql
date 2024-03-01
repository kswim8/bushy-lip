SELECT t2.t_name AS "relatedTag.name"
     , count(*) AS count
  FROM tag t INNER JOIN message_tag pt ON (t.t_tagid = pt.mt_tagid)
             INNER JOIN message c      ON (pt.mt_messageid = c.m_c_replyof)
             INNER JOIN message_tag ct ON (c.m_messageid = ct.mt_messageid)
             INNER JOIN tag t2      ON (ct.mt_tagid = t2.t_tagid)
             LEFT  JOIN message_tag nt ON (c.m_messageid = nt.mt_messageid AND nt.mt_tagid = pt.mt_tagid)
 WHERE 1=1
   AND nt.mt_messageid IS NULL
   AND t.t_name = 'Enrique_Iglesias'
 GROUP BY t2.t_name
 ORDER BY count DESC, t2.t_name
 LIMIT 100
;
