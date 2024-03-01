WITH RECURSIVE friends(startPerson, hopCount, friend) AS (
    SELECT p_personid, 0, p_personid
      FROM person
     WHERE 1=1
       AND p_personid = 19791209310731
  UNION
    SELECT f.startPerson
         , f.hopCount+1
         , CASE WHEN f.friend = k.k_person1id then k.k_person2id ELSE k.k_person1id END
      FROM friends f
         , knows k
     WHERE 1=1
       AND f.friend = k.k_person1id
       AND f.hopCount < 5
)
   , friends_shortest AS (
    SELECT startPerson, min(hopCount) AS hopCount, friend
      FROM friends
     GROUP BY startPerson, friend
)
   , friend_list AS (
    SELECT DISTINCT f.friend AS friendid
      FROM friends_shortest f
         , person tf 
         , place ci 
         , place co 
     WHERE 1=1
       AND f.friend = tf.p_personid
       AND tf.p_placeid = ci.pl_placeid
       AND ci.pl_containerplaceid = co.pl_placeid
       AND f.hopCount BETWEEN 3 AND 5
       AND co.pl_name = 'Pakistan'
)
   , messages_of_tagclass_by_friends AS (
    SELECT DISTINCT f.friendid
         , m.m_messageid AS messageid
      FROM friend_list f
         , message m
         , message_tag pt
         , tag t
         , tagclass tc
     WHERE 1=1
       AND f.friendid = m.m_creatorid
       AND m.m_messageid = pt.mt_messageid
       AND pt.mt_tagid = t.t_tagid
       AND t.t_tagclassid = tc.tc_tagclassid
       AND tc.tc_name = 'MusicalArtist'
)
SELECT m.friendid AS "person.id"
     , t.t_name AS "tag.name"
     , count(*) AS messageCount
  FROM messages_of_tagclass_by_friends m
     , message_tag pt
     , tag t
 WHERE 1=1
   AND m.messageid = pt.mt_messageid
   AND pt.mt_tagid = t.t_tagid
 GROUP BY m.friendid, t.t_name
 ORDER BY messageCount DESC, t.t_name, m.friendid
 LIMIT 100
;
