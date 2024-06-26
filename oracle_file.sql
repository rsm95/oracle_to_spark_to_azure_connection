create table datafile33
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

select * from datafile33;

describe datafile30;

ALTER TABLE datafile33
ADD id varchar2(10);

CREATE SEQUENCE id_seq33
START WITH 1
INCREMENT BY 1;

UPDATE datafile33
SET id = id_seq33.NEXTVAL;

select count(*) from datafile30;

ALTER TABLE datafile30
MODIFY COLUMN ID INTEGER;

SELECT NAME FROM V$SERVICES;

SELECT SYS_CONTEXT('USERENV','SERVER_HOST') FROM DUAL;

SELECT HOST_NAME FROM V$INSTANCE;
