WITH persons_of_country_w_friends AS (
    SELECT p.p_personid AS personid
         , k.k_person2id as friendid
      FROM person p
         , place ci 
         , place co 
         , knows k
     WHERE 1=1
       AND p.p_placeid = ci.pl_placeid
       AND ci.pl_containerplaceid = co.pl_placeid
       AND p.p_personid = k.k_person1id
       AND co.pl_name = 'Belarus'
)
SELECT count(*)
  FROM persons_of_country_w_friends p1
     , persons_of_country_w_friends p2
     , persons_of_country_w_friends p3
 WHERE 1=1
   AND p1.friendid = p2.personid
   AND p2.friendid = p3.personid
   AND p3.friendid = p1.personid
   AND p1.personid < p2.personid
   AND p2.personid < p3.personid
;
