extern crate rspg;
use rspg::{ConnInfo, PgError, Pgdb};

fn main() -> Result<(), PgError> {
    //  let conninfo = ConnInfo::frompart("host=127.0.0.1, database=test, user=test, password=1qaz.");
    let conninfo = ConnInfo::parse("rspg://test:1qaz.@127.0.0.1/test");
    println!("connect info = {}\r\n\r\n", conninfo);

    let mut conn = match Pgdb::connect(&conninfo) {
        Ok(v) => v,
        Err(err) => return Err(err),
    };
    println!("parameter : {:?}\r\n", conn.parameter);

    let mut cur = conn.cursor();
    _ = cur.begin();
// delete  from  users where id=100 returning id,name;
// insert into test(id,name) values(200,'abcd') returning id,name;
    match cur.query("delete   from users;") {
        Ok(_) => (),
        Err(err) => {
            _ = cur.rollback();
            return Err(err);
        }
    }
    _ = cur.rollback(); //commit();

    println!("lastmsg = {}\r\n", cur.lastmsg);
    println!("description = {}\r\n\r\n", cur.description);
    println!("rowcount = {}\r\n,  data = {}\r\n\r\n", cur.rowcount, cur.rows);

    for i in 0..cur.description.len() {
        println!("Column = {}:{}", cur.description[i].name, cur.description[i].typid);
    }

    for i in 0..cur.rows.len() {
       println!("row = {}", cur.rows[i]);
    }


    for col in cur.description.iter() {
        println!("description:idx ={}, name={},type={}", col.col, col.name, col.typid);
    }

    for row in cur.rows.iter() {
        println!("rows: len={},row={}", row.len(), row);
    }

    cur.close();
    return Ok(());
}
