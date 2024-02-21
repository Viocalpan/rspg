# rspg
rspg tiny native postgresql driver by rust
come soon connect pool

md5   https://github.com/stainless-steel/md5

base64  https://github.com/viocalpan/rbase64

scramSha256  https://github.com/sfackler/rust-postgres/blob/master/postgres-protocol/src/authentication/sasl.rs

test table struct(postgresql)
```
create table users(
id integer,
name varchar(8)
);

CREATE OR REPLACE FUNCTION  tf_users_i() RETURNS trigger AS $$
BEGIN
   if (new.id > 100) then
       raise exception 'user id (%) cannot be greater than 100!',new.id;
      RETURN null;
  end if;

   RETURN NEW;
  END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER  tr_users_i BEFORE INSERT ON users FOR EACH ROW EXECUTE PROCEDURE tf_users_i();


CREATE OR REPLACE FUNCTION  tf_users_d() RETURNS trigger AS $$
BEGIN
    raise NOTICE 'user name is "{%}" delete!', old.name;
  RETURN old;
  END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER  tr_users_d BEFORE DELETE ON users FOR EACH ROW EXECUTE PROCEDURE tf_users_d();
```
