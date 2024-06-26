create table datafile30
(country varchar2(10),
state varchar2(10),
table_number varchar2(100),
effective_date date,
expiration_date date,
key1 varchar2(100),
key2 varchar2(100),
key3 varchar2(100),
key4 varchar2(100),
factor decimal(10,2));

select * from datafile30;

describe datafile30;

ALTER TABLE datafile30
ADD id VARCHAR2(10);

CREATE SEQUENCE id_seq
START WITH 1
INCREMENT BY 1;

UPDATE datafile30
SET id = 'a' || id_seq.NEXTVAL;

select count(*) from datafile30;

ALTER TABLE datafile30
MODIFY COLUMN ID INTEGER;

