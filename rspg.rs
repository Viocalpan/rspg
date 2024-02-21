use std::collections::HashMap;
use std::error::Error as _Error;
use std::fmt;
use std::io::{Read as ioread, Write as iowrite};
use std::net::{Shutdown, TcpStream};

// backend
const AUTHENTICATIONOK: u8 = b'R'; //  MD5Password SASLFinal SASLContinue SASL GSSContinue SSPI GSS SCMCredential  CleartextPassword KerberosV5
const NEGOTIATEPROTOCOLVERSION: u8 = b'v';
const BACKENDKEYDATA: u8 = b'K';
const PARAMETERSTATUS: u8 = b'S';
const ERRORRESPONSE: u8 = b'E';
const NOTICERESPONSE: u8 = b'N';
const NOTIFICATIONRESPONSE: u8 = b'A';
const READYFORQUERY: u8 = b'Z';

const EMPTYQUERYRESPONSE: u8 = b'I';
const ROWDESCRIPTION: u8 = b'T';
const DATAROW: u8 = b'D';
const COMMANDCOMPLETE: u8 = b'C';

// frontend
const PASSWORDMESSAGE: u8 = b'p';
const QUERY: u8 = b'Q';
const TERMINATE: u8 = b'X';

//  ErrorMessage
//  CancelRequest
//  StartupMessage

// -------------------------------------------------------------
pub struct ConnInfo<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub password: &'a str,
    pub database: &'a str,
    pub appname: &'a str,
}

impl Default for ConnInfo<'_> {
    fn default() -> Self {
        ConnInfo {
            host: "127.0.0.1",
            port: 5432,
            user: "test",
            password: "",
            database: "test",
            appname: "rspg",
        }
    }
}

impl fmt::Display for ConnInfo<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"{{"host": "{}", "port": {}, "user": "{}", "password": "********", "database": "{}", "appname": "{}"}}"#,
            self.host, self.port, self.user, self.database, self.appname
        )
    }
}

impl<'a> ConnInfo<'a> {
    //  parse("appname://test:passw0rd@host:5432/test");
    //  frompart("host=localhost, port=5432, user=test, password=passw0rd, database=test, appname=rspg")

    pub fn parse(dburl: &'a str) -> Self {
        let levelurl: &str;
        let mut upath: &str = "/";
        let mut netloc: &str;
        let mut defvalue = ConnInfo::default();

        match dburl.find("://") {
            Some(i) => {
                defvalue.appname = &dburl[..i]; //  scheme
                levelurl = &dburl[i + 3..];
            }
            _ => levelurl = &dburl,
        }
        match levelurl.find("/") {
            Some(i) => {
                netloc = &levelurl[..i];
                upath = &levelurl[i..];

                match upath.find(";") {
                    Some(i) => upath = &upath[..i],
                    _ => (),
                }
                match upath.find("?") {
                    Some(i) => upath = &upath[..i],
                    _ => (),
                }
                match upath.find("#") {
                    Some(i) => upath = &upath[..i],
                    _ => (),
                }
            }
            _ => netloc = &levelurl,
        }

        match netloc.find("@") {
            Some(i) => {
                let authent = &netloc[..i];
                netloc = &netloc[i + 1..];
                match authent.find(":") {
                    Some(i) => {
                        defvalue.user = &authent[..i];
                        defvalue.password = &authent[i + 1..];
                    }
                    _ => defvalue.user = &authent,
                }
            }
            _ => (),
        }

        match netloc.find(":") {
            Some(i) => {
                defvalue.host = &netloc[..i];
                let sport = netloc[i + 1..].parse::<u16>();
                match sport {
                    Ok(v) => {
                        if v > 10 {
                            defvalue.port = v;
                        }
                    }
                    Err(_) => (),
                }
            }
            _ => {
                if netloc.len() > 0 {
                    defvalue.host = &netloc;
                }
            }
        }

        loop {
            match upath.find("/") {
                Some(i) => {
                    if i == 0 {
                        upath = &upath[1..];
                        continue;
                    }
                    upath = &upath[..i];
                }
                _ => break,
            }
        }

        if upath.len() > 0 {
            defvalue.database = upath; //  path
        }

        defvalue
    }

    pub fn frompart(params: &'a str) -> Self {
        let body_kv = parse_dict(&params);
        let mut defvalue = ConnInfo::default();

        match body_kv.get("host") {
            Some(v) => defvalue.host = v,
            _ => {}
        }

        match body_kv.get("port") {
            Some(v) => {
                let sport = v.parse::<u16>();
                match sport {
                    Ok(v) => {
                        if v > 10 {
                            defvalue.port = v;
                        }
                    }
                    Err(_) => {}
                }
            }
            _ => {}
        }

        match body_kv.get("user") {
            Some(v) => defvalue.user = v,
            _ => {}
        };

        match body_kv.get("password") {
            Some(v) => defvalue.password = v,
            _ => {}
        }

        match body_kv.get("database") {
            Some(v) => defvalue.database = v,
            _ => {}
        }

        match body_kv.get("appname") {
            Some(v) => defvalue.appname = v,
            _ => {}
        }

        defvalue
    }
}

// -----------------------------
#[derive(PartialEq)]
enum Pgstatus {
    Undetermined = -1,
    Closed = 0,
    Abort = -2,
    Active = 256,
}

#[derive(PartialEq)]
enum Transtatus {
    Idle = b'I' as isize,
    Fail = b'E' as isize,
    Ing = b'T' as isize,
}

impl Transtatus {
    pub fn from(t: u8) -> Result<Self, PgError> {
        let rtrans: Transtatus;
        match t {
            b'I' => rtrans = Transtatus::Idle,
            b'E' => rtrans = Transtatus::Fail,
            b'T' => rtrans = Transtatus::Ing,
            _ => return Err(PgError::NotSupportedError(format!("Invalid transaction status({})", t as char))),
        }
        Ok(rtrans)
    }
}

// -------------------------------------------------------------
pub struct Pgdb {
    netstream: std::net::TcpStream,
    backend: i32, //  status (UNDETERMINED, -1 |ACTIVE, >=256 |CLOSE, 0|ABORT, -2)
    secret: i32,
    transaction: Transtatus, //  (IDLE = 'I' ING = 'T' FAILED = 'E')
    pub parameter: HashMap<String, String>,
    pub lastmsg: ReplyMsg,
    svrinfo: String,
}

impl Drop for Pgdb {
    fn drop(&mut self) {
        self.close();
    }
}

impl Pgdb {
    pub fn connect(conninfo: &ConnInfo) -> Result<Self, PgError> {
        let rs: String;
        let stream: TcpStream;
        let (host, port) = (conninfo.host.to_string(), conninfo.port);
        let (user, database, password, appname) = (conninfo.user, conninfo.database, conninfo.password, conninfo.appname);
        let svrinfo: String;
        svrinfo = format!("{}:{}", host, port);
        stream = match TcpStream::connect(&svrinfo) {
            Ok(v) => v,
            Err(_) => {
                rs = format!(r#"Connect to server {} fail"#, svrinfo);
                return Err(PgError::CouldNotConnectToServer(rs));
            }
        };

        match stream.set_nodelay(true) {
            Ok(_) => {}
            Err(_) => {}
        }
        let mut this = Self {
            backend: Pgstatus::Undetermined as i32,
            secret: 0,
            transaction: Transtatus::Idle,
            parameter: [].into(),
            netstream: stream,
            lastmsg: ReplyMsg::new(),
            svrinfo: svrinfo,
        };

        match this.startup(user, database, appname) {
            Ok(_) => {}
            Err(err) => return Err(err),
        }

        match this.authenticate(user, &password) {
            Ok(_) => {}
            Err(err) => return Err(err),
        }

        match this.readyforquery() {
            Ok(_) => {}
            Err(err) => return Err(err),
        }

        this.parameter.insert("host".to_string(), host);
        this.parameter.insert("port".to_string(), port.to_string());
        this.parameter.insert("user".to_string(), user.to_string());
        this.parameter.insert("dbname".to_string(), database.to_string());
        Ok(this)
    }

    pub fn close(&mut self) {
        if self.backend > Pgstatus::Active as i32 {
            _ = self.sendata(TERMINATE, "".into());
            self.backend = Pgstatus::Closed as i32;
        }

        match self.netstream.shutdown(Shutdown::Both) {
            Ok(_) => {}
            Err(_) => {}
        }
    }

    fn startup(&mut self, user: &str, database: &str, appname: &str) -> Result<(), PgError> {
        let mut rbuf: Vec<u8> = vec![];
        let pver = put_i32(196_608); //  protocol version { 0, 3, 0, 0 }
        rbuf.extend_from_slice(&pver);
        rbuf.extend_from_slice("user\x00".as_bytes());
        rbuf.extend_from_slice(user.as_bytes());
        rbuf.push(0);
        rbuf.extend_from_slice("database\x00".as_bytes());
        rbuf.extend_from_slice(database.as_bytes());
        rbuf.push(0);
        rbuf.extend_from_slice("application_name\x00".as_bytes());
        rbuf.extend_from_slice(appname.as_bytes());
        rbuf.push(0);
        rbuf.extend_from_slice("client_encoding\x00UTF8\x00".as_bytes());
        rbuf.push(0);
        self.sendata(0, rbuf)
    }

    fn authenticate(&mut self, user: &str, password: &str) -> Result<(), PgError> {
        let rs: String;
        let mut code = [0u8; 1];
        let mut lenbuf = [0u8; 4];
        let mut nomsgf: bool = false;
        match self.netstream.read(&mut code) {
            Ok(v) => v,
            Err(_) => {
                rs = format!(r#"Read from server {} fail"#, self.svrinfo);
                return Err(PgError::LostConnectionToServer(rs));
            }
        };

        match self.netstream.read(&mut lenbuf) {
            Ok(v) => v,
            Err(_) => {
                rs = format!(r#"Read from server {} fail"#, self.svrinfo);
                return Err(PgError::LostConnectionToServer(rs));
            }
        };

        let mut len = get_i32(lenbuf.to_vec(), 0);
        if len > 4096 {
            //  1178686529 b'E,FATA,L
            nomsgf = true;
            len = 128;
        }

        let mut buffer = vec![0; len as usize - 4];
        match self.netstream.read(&mut buffer) {
            Ok(v) => v,
            Err(_) => {
                rs = format!(r#"Read from server {} fail"#, self.svrinfo);
                return Err(PgError::LostConnectionToServer(rs));
            }
        };

        if nomsgf == true {
            //  b'EFATAL:  unsupported frontend protocol 0.12345: server supports 2.0 to 3.0\n\x00'
            let mut ers = String::new();
            for i in 0..4 {
                ers.push(lenbuf[i] as char);
            }

            let mut i = 0;
            while (buffer[i] != 0) && (i < len as usize - 4) {
                ers.push(buffer[i] as char);
                i += 1;
            }
            return Err(PgError::NotSupportedError(ers));
        }

        let mut authmsg = MsgData {
            code: code[0],
            pos: 0,
            data: buffer[0..(len as usize) - 4].to_vec(),
        };

        let mut scram: ScramSha256 = ScramSha256 {
            buffer: String::from(""),
            state: ScramState::Done,
        };

        let mut hasdata: Vec<u8>;
        loop {
            if authmsg.code != AUTHENTICATIONOK {
                if authmsg.code == ERRORRESPONSE {
                    //  b'E\x00\x00\x00\x88SFATAL\x00VFATAL\x00C0A000\x00Munsupported frontend protocol 29.65474: server supports 2.0 to 3.0\x00Fpostmaster.c\x00L2120\x00RProcessStartupPacket\x00\x00'
                    let errmsg = ReplyMsg::parseresponse(authmsg);
                    let rseverity = errmsg.severity;
                    rs = format!(r#"{}"#, errmsg.message);
                    let rcode = errmsg.code;
                    return Err(PgError::InternalError {
                        severity: rseverity,
                        code: rcode,
                        msg: rs,
                    });
                }
                rs = format!(r#"Inviold database server {}"#, self.svrinfo);
                return Err(PgError::NotSupportedError(rs));
            }

            let password_type = authmsg.read_i32();
            match password_type {
                0 => {
                    //  authenticationok
                    return Ok(());
                }

                3 => {
                    //  plain-text
                    if password.len() == 0 {
                        rs = format!("Please provide the password for user({})", user);
                        return Err(PgError::AuthenticationFail(rs));
                    }
                    hasdata = vec![];
                    hasdata.extend_from_slice(password.as_bytes());
                    hasdata.push(0);
                    _ = self.sendata(PASSWORDMESSAGE, hasdata);
                }

                5 => {
                    //  md5
                    if password.len() == 0 {
                        rs = format!("Please provide the password for user({})", user);
                        return Err(PgError::AuthenticationFail(rs));
                    }
                    let salt = authmsg.read_bytes(4);
                    let md5 = pghashmd5(user, password, salt);
                    hasdata = vec![];
                    hasdata.extend_from_slice(md5.as_bytes());
                    hasdata.push(0);
                    _ = self.sendata(PASSWORDMESSAGE, hasdata);
                }

                10 => {
                    //  sasl AuthenticationSASL
                    if password.len() == 0 {
                        rs = format!("Please provide the password for user({})", user);
                        return Err(PgError::AuthenticationFail(rs));
                    }
                    scram = ScramSha256::new(password.as_bytes());
                    hasdata = scram.genhashdata();
                    _ = self.sendata(PASSWORDMESSAGE, hasdata);
                }

                11 => {
                    //  sasl AuthenticationSASLContinue
                    match scram.update(&authmsg.data[4..]) {
                        Ok(_) => {}
                        Err(err) => {
                            rs = format!(r#"{}"#, err);
                            return Err(PgError::AuthenticationError(rs));
                        }
                    }
                    hasdata = scram.genhashdata();
                    _ = self.sendata(PASSWORDMESSAGE, hasdata);
                }

                12 => {
                    //  sasl AuthenticationSASLFinal
                    match scram.finish(&authmsg.data[4..]) {
                        Ok(_) => {}
                        Err(err) => {
                            rs = format!(r#"{}"#, err);
                            return Err(PgError::AuthenticationError(rs));
                        }
                    }
                }

                _ => {
                    rs = format!("Not support authentication method for server({})", self.svrinfo);
                    return Err(PgError::NotSupportAuthenticationMethod(rs));
                }
            }

            authmsg = match self.readata() {
                Ok(v) => v,
                Err(err) => return Err(err),
            };
        }
    }

    fn readyforquery(&mut self) -> Result<(), PgError> {
        let mut k: String;
        let mut v: String;
        loop {
            let mut msg = match self.readata() {
                Ok(v) => v,
                Err(err) => return Err(err),
            };
            match msg.code {
                PARAMETERSTATUS => {
                    k = msg.read_strings().to_string();
                    v = msg.read_strings().to_string();
                    self.parameter.insert(k, v);
                }
                BACKENDKEYDATA => {
                    self.backend = msg.read_i32();
                    self.secret = msg.read_i32();
                }
                ERRORRESPONSE => {
                    //  b'E\x00\x00\x00gSFATAL\x00VFATAL\x00C42704\x00Munrecognized configuration parameter "pgdb"\x00Fguc.c\x00L6759\x00Rset_config_option\x00\x00'
                    let errmsg = ReplyMsg::parseresponse(msg);
                    let rseverity = errmsg.severity;
                    let rs = format!(r#"{}"#, errmsg.message);
                    let rcode = errmsg.code;
                    return Err(PgError::InternalError {
                        severity: rseverity,
                        code: rcode,
                        msg: rs,
                    });
                }
                NOTICERESPONSE => {
                    self.lastmsg = ReplyMsg::parseresponse(msg);
                }
                READYFORQUERY => {
                    let t = msg.data[0];
                    self.transaction = match Transtatus::from(t) {
                        Ok(v) => v,
                        Err(err) => return Err(err),
                    };
                    break;
                }
                NEGOTIATEPROTOCOLVERSION => {}
                _ => {
                    let rs = format!("Invalid database server({})", self.svrinfo);
                    return Err(PgError::NotSupportedError(rs));
                }
            }
        }

        Ok(())
    }

    pub fn cursor(&mut self) -> Cursor {
        return Cursor::init(self);
    }

    pub fn cancel(&mut self) -> Result<(), PgError> {
        let mut rbuf: Vec<u8> = vec![];
        let mut mdata = put_i32(80877102);
        rbuf.extend_from_slice(&mdata);
        mdata = put_i32(self.backend);
        rbuf.extend_from_slice(&mdata);
        mdata = put_i32(self.secret);
        rbuf.extend_from_slice(&mdata);
        self.sendata(0, rbuf)
    }

    fn sendata(&mut self, code: u8, payload: Vec<u8>) -> Result<(), PgError> {
        let datalen = payload.len();
        let mut rbuf: Vec<u8> = vec![];
        let mut rcode = [0u8; 1];
        rcode[0] = code;
        if code > 0 {
            rbuf.extend_from_slice(&rcode);
        }
        let mlen = put_i32((datalen + 4) as i32);
        rbuf.extend_from_slice(&mlen);
        rbuf.extend_from_slice(&payload);
        let rs = format!("Write to server {} fail", self.svrinfo);
        match self.netstream.write(&rbuf) {
            Ok(_) => {}
            Err(_) => {
                if self.backend > Pgstatus::Active as i32 {
                    self.backend = Pgstatus::Abort as i32;
                }
                return Err(PgError::LostConnectionToServer(rs));
            }
        };
        match self.netstream.flush() {
            Ok(_) => {}
            Err(_) => {
                if self.backend > Pgstatus::Active as i32 {
                    self.backend = Pgstatus::Abort as i32;
                }
                return Err(PgError::LostConnectionToServer(rs));
            }
        };
        Ok(())
    }

    fn readata(&mut self) -> Result<MsgData, PgError> {
        let mut code = [0u8; 1];
        let mut recvlen = [0u8; 4];
        let rs = format!("Read from server {} fail", self.svrinfo);
        match self.netstream.read(&mut code) {
            Ok(_) => {}
            Err(_) => {
                if self.backend > Pgstatus::Active as i32 {
                    self.backend = Pgstatus::Abort as i32;
                }
                return Err(PgError::LostConnectionToServer(rs));
            }
        }
        match self.netstream.read(&mut recvlen) {
            Ok(_) => {}
            Err(_) => {
                if self.backend > Pgstatus::Active as i32 {
                    self.backend = Pgstatus::Abort as i32;
                }
                return Err(PgError::LostConnectionToServer(rs));
            }
        }
        let mlen: i32 = get_i32(recvlen.to_vec(), 0) as i32 - 4;
        if mlen > 0 {
            let mut rbuf = vec![0; mlen as usize]; //  Vec::new();
            match self.netstream.read(&mut rbuf) {
                Ok(_) => {}
                Err(_) => {
                    if self.backend > Pgstatus::Active as i32 {
                        self.backend = Pgstatus::Abort as i32;
                    }
                    return Err(PgError::LostConnectionToServer(rs));
                }
            }

            return Ok(MsgData {
                code: code[0],
                data: rbuf.clone(),
                pos: 0,
            });
        }
        return Ok(MsgData {
            code: code[0],
            data: Vec::new(),
            pos: 0,
        });
    }
}

// -------------------------------------------------------------
pub struct Cursor<'a> {
    conn: &'a mut Pgdb,
    pub description: Columns,
    pub rows: Datarows,
    pub rowcount: i32,
    pub lastmsg: ReplyMsg,
    status: Pgstatus,
}

impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        //  self.close();
    }
}

impl Cursor<'_> {
    fn init(pgconn: &mut Pgdb) -> Cursor {
        Cursor {
            conn: pgconn,
            description: Columns(vec![]),
            rows: Datarows(vec![]),
            rowcount: 0,
            lastmsg: ReplyMsg::new(),
            status: Pgstatus::Active,
        }
    }

    pub fn close(&mut self) {
        if self.status == Pgstatus::Active {
            self.status = Pgstatus::Closed;
        }
        self.description = Columns(vec![]);
        self.rows = Datarows(vec![]);
        self.rowcount = 0;
    }

    fn islive(&mut self) -> Result<(), PgError> {
        if self.status != Pgstatus::Active {
            return Err(PgError::CursorClosed);
        }
        if self.conn.backend < Pgstatus::Active as i32 {
            return Err(PgError::ConnectionClosed);
        }
        Ok(())
    }

    pub fn begin(&mut self) -> Result<(), PgError> {
        if self.conn.transaction != Transtatus::Ing {
            return self.exec("START TRANSACTION;"); //  BEGIN;
        }
        return Err(PgError::TransactionIdle);
    }

    pub fn commit(&mut self) -> Result<(), PgError> {
        if self.conn.transaction == Transtatus::Ing {
            return self.exec("COMMIT;");
        }
        return Err(PgError::TransactionFail);
    }

    pub fn rollback(&mut self) -> Result<(), PgError> {
        if self.conn.transaction != Transtatus::Idle {
            return self.exec("ROLLBACK;");
        }
        return Err(PgError::TransactionFail);
    }

    pub fn exec(&mut self, sqlstr: &str) -> Result<(), PgError> {
        match self.islive() {
            Ok(_) => {}
            Err(err) => return Err(err),
        }
        let mut erm: bool = false;

        if self.conn.transaction == Transtatus::Idle {
            self.rowcount = 0;
        };

        let mut rbuf: Vec<u8> = vec![];
        rbuf.extend_from_slice(sqlstr.as_bytes());
        rbuf.push(0);
        _ = self.conn.sendata(QUERY, rbuf);

        loop {
            let mut msg = match self.conn.readata() {
                Ok(v) => v,
                Err(err) => return Err(err),
            };

            match msg.code {
                ERRORRESPONSE => {
                    erm = true;
                    self.lastmsg = ReplyMsg::parseresponse(msg);
                }
                NOTICERESPONSE => {
                    self.lastmsg = ReplyMsg::parseresponse(msg);
                }
                NOTIFICATIONRESPONSE => {}
                ROWDESCRIPTION => {}
                DATAROW => {}
                EMPTYQUERYRESPONSE => {}
                COMMANDCOMPLETE => {
                    let rcnt = parsecommandcomplete(msg.read_strings());
                    if rcnt > 0 {
                        self.rowcount = rcnt;
                    }
                }
                READYFORQUERY => {
                    let t = msg.data[0];
                    self.conn.transaction = match Transtatus::from(t) {
                        Ok(v) => v,
                        Err(err) => return Err(err),
                    };
                    break;
                }
                _ => {} //  PARAMETERSTATUS
            }
        }
        if erm {
            let errmsg = &self.lastmsg;
            let rseverity = &errmsg.severity;
            let rcode = &errmsg.code;
            let rs = format!(r#"{}"#, errmsg.message);
            return Err(PgError::SqlExecuteError {
                severity: rseverity.to_string(),
                code: rcode.to_string(),
                msg: rs,
            });
        }
        Ok(())
    }

    pub fn query(&mut self, sqlstr: &str) -> Result<(), PgError> {
        match self.islive() {
            Ok(_) => {}
            Err(err) => return Err(err),
        }
        let mut erm: bool = false;

        if self.conn.transaction == Transtatus::Idle {
            self.rowcount = 0;
        };

        let mut rbuf: Vec<u8> = vec![];
        rbuf.extend_from_slice(sqlstr.as_bytes());
        rbuf.push(0);
        _ = self.conn.sendata(QUERY, rbuf);

        let mut fieldtyp: Vec<i32> = vec![];
        loop {
            let mut msg = match self.conn.readata() {
                Ok(v) => v,
                Err(err) => return Err(err),
            };
            match msg.code {
                ERRORRESPONSE => {
                    erm = true;
                    self.lastmsg = ReplyMsg::parseresponse(msg);
                }
                NOTICERESPONSE => {
                    self.lastmsg = ReplyMsg::parseresponse(msg);
                }
                NOTIFICATIONRESPONSE => {}
                ROWDESCRIPTION => {
                    self.description = parserowdescription(&mut msg);
                    fieldtyp = self.description.iter().map(|col| col.typid).collect();
                }
                DATAROW => {
                    let row = parsedatarow(&mut msg, &fieldtyp);
                    self.rows.0.push(row);
                }
                EMPTYQUERYRESPONSE => {}
                COMMANDCOMPLETE => {
                    let rcnt = parsecommandcomplete(msg.read_strings());
                    if rcnt > 0 {
                        self.rowcount = rcnt;
                    }
                }
                READYFORQUERY => {
                    let t = msg.data[0];
                    self.conn.transaction = match Transtatus::from(t) {
                        Ok(v) => v,
                        Err(err) => return Err(err),
                    };
                    break;
                }
                _ => {} //  PARAMETERSTATUS
            }
        }
        if erm {
            let errmsg = &self.lastmsg;
            let rseverity = &errmsg.severity;
            let rcode = &errmsg.code;
            let rs = format!(r#"{}"#, errmsg.message);
            return Err(PgError::SqlExecuteError {
                severity: rseverity.to_string(),
                code: rcode.to_string(),
                msg: rs,
            });
        }
        Ok(())
    }
}

// -------------------------------------------------------------
#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub col: i16,
    pub typid: i32,
    typlen: i16,
    typmod: i32,
    //  fmtcode: i16, (0,text|1 binary)
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fieldlen: i32;
        if (self.typlen > 0) || (self.typid == 1700) {
            fieldlen = self.typlen as i32
        } else {
            fieldlen = self.typmod
        }
        write!(f, r#"(name: "{}", type: {}, size: {})"#, self.name, Self::pgtypdisp(self.typid, self.typmod), fieldlen)
    }
}

impl Column {
    fn pgtypdisp(fdtyp: i32, fdmod: i32) -> String {
        let t: &str;
        let st: String;
        match fdtyp {
            16 => t = "bool",
            18 => t = "char",
            20 => t = "int8 (bigint)",
            21 => t = "int2 (smallint)",
            23 => t = "int4 (integer)",
            25 => t = "text",
            114 => t = "json",
            142 => t = "xml",
            650 => t = "cidr",
            700 => t = "float4 (real)",
            701 => t = "float8 (double precision)",
            790 => t = "money",
            829 => t = "macaddr",
            869 => t = "inet",
            1042 => {
                st = format!("char({})", fdmod - 4);
                t = &st;
            } //  t = "char", //char(n) (character(n))
            1043 => {
                st = format!("varchar({})", fdmod - 4);
                t = &st;
            } //  t = "varchar", //varchar(n) (character varying())
            1082 => t = "date",
            1083 => t = "time (time without time zone)",
            1114 => t = "timestamp (timestamp without time zone)",
            1184 => t = "timestamptz (timestamp with time zone)",
            1266 => t = "timetz (time with time zone)",
            1700 => {
                let (p, s) = Self::parsnumeric(fdmod);
                st = format!("numeric({},{})", p, s);
                t = &st;
            } //  t = "numeric", //numeric(p,s)
            2950 => t = "uuid",
            _ => {
                st = format!("pg_type({})", fdtyp);
                t = &st
            }
        }
        return t.to_string();
    }

    fn parsnumeric(tymod: i32) -> (u16, u16) {
        let x = tymod - 4;
        return ((x / 65536) as u16, (x % 65536) as u16);
    }
}

#[derive(Clone)]
pub struct Columns(Vec<Column>);

impl fmt::Display for Columns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        _ = write!(f, r"[");
        for col in self.0.iter() {
            _ = write!(f, r"{},", col);
        }
        _ = write!(f, r"]");
        Ok(())
    }
}

impl Deref for Columns {
    type Target = Vec<Column>;
    fn deref(&self) -> &Vec<Column> {
        &self.0
    }
}

fn parserowdescription(msg: &mut MsgData) -> Columns {
    let colnum = msg.read_i16();
    let mut collist = vec![]; //  (Column{name: String, col: i16, typid: i32, typlen: i16, typmod: i32 });
    let mut coln: i16;
    let mut dtyp: i32;
    let mut dlen: i16;
    let mut dmod: i32;
    let mut name: String;
    for i in 0..colnum {
        name = msg.read_strings().to_string();
        if (name == "?") || (name == "") || (name == "?column?") {
            name = format!(r#"column{:0>2}"#, i);
        }
        _ = msg.read_bytes(4); //  read_i32();
        coln = msg.read_i16();
        dtyp = msg.read_i32();
        dlen = msg.read_i16();
        dmod = msg.read_i32();
        _ = msg.read_bytes(2);

        collist.push(Column {
            name: name.clone(),
            col: coln,
            typid: dtyp,
            typlen: dlen,
            typmod: dmod,
        });
    }
    return Columns(collist);
}

// -----------------------------
#[derive(Clone)]
pub struct Dcol {
    pub value: String,
    pub idx: i16,
    pub size: i32,
    pub typid: i32,
}

impl fmt::Display for Dcol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r#"value: {},"#, self.value,)
    }
}

#[derive(Clone)]
pub struct Drow(Vec<Dcol>);

impl Deref for Drow {
    type Target = Vec<Dcol>;
    fn deref(&self) -> &Vec<Dcol> {
        &self.0
    }
}

impl fmt::Display for Drow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        _ = write!(f, r"(");
        for dcol in self.0.iter() {
            if (dcol.size == -1) || ((dcol.size > 0) && ((dcol.typid == 16) || (dcol.typid == 20) || (dcol.typid == 21) || (dcol.typid == 23) || (dcol.typid == 700) || (dcol.typid == 701) || (dcol.typid == 790) || (dcol.typid == 1700))) {
                _ = write!(f, r#"{},"#, dcol.value);
            } else {
                _ = write!(f, r#""{}","#, dcol.value);
            }
        }
        _ = write!(f, r")");
        Ok(())
    }
}

#[derive(Clone)]
pub struct Datarows(Vec<Drow>);

impl Deref for Datarows {
    type Target = Vec<Drow>;
    fn deref(&self) -> &Vec<Drow> {
        &self.0
    }
}

impl fmt::Display for Datarows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        _ = write!(f, r"[");
        for drow in self.0.iter() {
            _ = write!(f, r"{},", drow);
        }
        _ = write!(f, r"]");
        Ok(())
    }
}

fn parsedatarow(msg: &mut MsgData, fieldtyp: &Vec<i32>) -> Drow {
    let mut valen: i32;
    let mut value: String;
    let colnum = msg.read_i16();
    let mut collist = vec![];
    //  (Dcol{ value: String, idx: i16, size: i32 });
    for i in 0..colnum as usize {
        valen = msg.read_i32();
        if valen == -1 {
            //  u32 0xFFFFFFFF i32 -1
            value = "null".to_string();
        } else {
            value = match String::from_utf8(msg.read_bytes(valen as usize)) {
                Ok(v) => v,
                Err(_) => "".to_string(),
            };
            if fieldtyp[i] == 16 {
                if value == "t" {
                    value = "true".to_string();
                }
                if value == "f" {
                    value = "false".to_string();
                }
            }
        }
        collist.push(Dcol {
            value: value.clone(),
            idx: i as i16,
            size: valen,
            typid: fieldtyp[i],
        });
    }
    return Drow(collist);
}

// -----------------------------
fn parsecommandcomplete(command: String) -> i32 {
    //  MOVE,FETCH,COPY
    //  INSERT,DELETE,UPDATE,SELECT
    let strcmd = command;
    if strcmd.len() == 0 {
        return 0;
    }
    let mut last: String = "-1".to_string();
    let iters: Vec<_> = strcmd.split(" ").collect();
    let first = iters[0];
    if (first == "INSERT") || (first == "DELETE") || (first == "UPDATE") || (first == "SELECT") {
        last = "0".to_string();
    }
    if iters.len() > 1 {
        last = iters[iters.len() - 1].to_string();
    }
    let scnt = String::from(&last).parse::<i32>();
    return match scnt {
        Ok(v) => v,
        Err(_) => 0,
    };
}

// -----------------------------
#[derive(Debug)]
pub enum PgError {
    CouldNotConnectToServer(String),
    LostConnectionToServer(String),
    NotSupportedError(String),
    InternalError {
        severity: String,
        code: String,
        msg: String,
    },
    NotSupportAuthenticationMethod(String),
    AuthenticationError(String),
    AuthenticationFail(String),
    ConnectionClosed,
    TransactionFail,
    TransactionIdle,
    CursorClosed,
    SqlExecuteError {
        severity: String,
        code: String,
        msg: String,
    },
    General(String),
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::CouldNotConnectToServer(ref rs) => write!(f, "CouldNotConnectToServer: {}", rs),
            Self::LostConnectionToServer(ref rs) => write!(f, "LostConnectionToServer: {}", rs),
            Self::NotSupportedError(ref rs) => write!(f, "NotSupportedError: {}", rs),
            Self::InternalError {
                ref severity,
                ref code,
                ref msg,
            } => write!(f, r#"InternalError: {{"severity":"{}", "code":"{}", "message": "{}"}}"#, severity, code, msg),
            Self::NotSupportAuthenticationMethod(ref rs) => write!(f, "NotSupportAuthenticationMethod: {}", rs),
            Self::AuthenticationError(ref rs) => write!(f, "AuthenticationError: {}", rs),
            Self::AuthenticationFail(ref rs) => write!(f, "AuthenticationFail: {}", rs),
            Self::ConnectionClosed => write!(f, "ConnectionClosed"),
            Self::CursorClosed => write!(f, "CursorClosed"),
            Self::SqlExecuteError {
                ref severity,
                ref code,
                ref msg,
            } => write!(f, r#"SqlExecuteError:  {{"severity":"{}", "code":"{}", "message": "{}"}}"#, severity, code, msg),
            Self::TransactionFail => write!(f, "TransactionNoActive"),
            Self::TransactionIdle => write!(f, "TransactionNoIdle"),
            Self::General(ref err) => write!(f, "UnexpectedError: {}", err),
        }
    }
}

impl _Error for PgError {}

// -----------------------------
pub struct ReplyMsg {
    code: String,
    severity: String, //  ERROR;FATAL;PANIC;WARNING;NOTICE;DEBUG;INFO;LOG
    message: String,
    //  hint: String,
    //  Where: String,
}

impl fmt::Display for ReplyMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r#"{{code: {}, {}: {}}}"#, self.code, self.severity, self.message)
    }
}

impl<'a> ReplyMsg {
    fn new() -> Self {
        Self {
            code: String::from("00000"),
            severity: String::from(""),
            message: String::from(""),
        }
    }

    fn parseresponse(mut msg: MsgData) -> ReplyMsg {
        let mut resp = ReplyMsg::new();
        let mut mtyp: u8;
        loop {
            mtyp = msg.read_u8();
            if mtyp == 0 {
                break;
            }
            match mtyp {
                b'C' => resp.code = msg.read_strings().to_string(),
                b'V' => resp.severity = msg.read_strings().to_string(),
                b'M' => resp.message = msg.read_strings().to_string(),
                _ => _ = msg.read_strings(),
            }
        }
        resp
    }
}

// -------------------------------------------------------------
struct MsgData {
    code: u8,
    data: Vec<u8>,
    pos: usize,
}

impl<'a> MsgData {
    fn read_i32(&mut self) -> i32 {
        let mut r: i32 = 0;
        if (self.data.len() - 4) >= self.pos {
            r = get_i32((self.data).to_vec(), self.pos);
            self.pos += 4;
        }
        return r;
    }

    fn read_i16(&mut self) -> i16 {
        let mut r: i16 = 0;
        if (self.data.len() - 2) >= self.pos {
            r = get_i16((self.data).to_vec(), self.pos);
            self.pos += 2;
        }
        return r;
    }

    fn read_u8(&mut self) -> u8 {
        let mut r: u8 = 0;
        if (self.data.len() - 1) >= self.pos {
            r = (self.data)[self.pos];
            self.pos += 1;
        }
        return r;
    }

    fn read_bytes(&mut self, num: usize) -> Vec<u8> {
        let mut r = vec![];
        let start = self.pos;
        if self.data.len() > start {
            let mut dlen = self.data.len() - start;
            if dlen > num {
                dlen = num;
            }
            r = self.data[start..start + dlen].to_vec();
            self.pos += dlen;
        }
        return r;
    }

    fn read_strings(&mut self) -> String {
        let start = self.pos;
        let mut r = vec![];
        if self.data.len() > start {
            while (self.data[self.pos] != 0) && (self.pos < self.data.len()) {
                self.pos += 1
            }
            self.pos += 1;
            r = self.data[start..self.pos - 1].to_vec();
        }
        match String::from_utf8(r) {
            Ok(v) => v,
            Err(_) => "invalid utf8 strings".to_string(),
        }
    }
}

// -----------------------------
#[inline]
fn put_i32(val: i32) -> Vec<u8> {
    return val.to_be_bytes().to_vec();
}

#[inline]
fn get_i32(buf: Vec<u8>, idx: usize) -> i32 {
    return ((buf[idx + 0] as i32) << 24) + ((buf[idx + 1] as i32) << 16) + ((buf[idx + 2] as i32) << 8) + (buf[idx + 3] as i32);
}

#[inline]
fn get_i16(buf: Vec<u8>, idx: usize) -> i16 {
    return ((buf[idx + 0] as i16) << 8) + (buf[idx + 1] as i16);
}

#[inline]
fn parse_dict(argstr: &str) -> HashMap<&str, &str> {
    let argvs = argstr.split(",");
    let argvec: Vec<&str> = argvs.collect();
    let kvlen = argvec.len();
    let mut keyv: HashMap<&str, &str> = HashMap::new();

    for i in 0..kvlen {
        let item = argvec[i];
        match item.find("=") {
            Some(i) => {
                let k = &item[..i].trim();
                let v = &item[i + 1..].trim();
                if k.len() > 0 && v.len() > 0 {
                    keyv.insert(k, v);
                };
            }
            None => (),
        }
    }
    keyv
}

// -----------------------------
fn pghashmd5(user: &str, password: &str, salt: Vec<u8>) -> String {
    let mut md5h = MD5::new();
    md5h.update(password.as_bytes().to_vec());
    md5h.update(user.as_bytes().to_vec());
    let hexr = digests("", &md5h.finish());
    md5h = MD5::new();
    let text_u8 = hexr.into_bytes();
    md5h.update(&text_u8);
    md5h.update(&salt);
    let hkey = md5h.finish();
    return digests("md5", &hkey);
}

fn digests(prev: &str, sum: &[u8; 16]) -> String {
    let mut r: String = prev.to_string();
    for b in sum {
        r = r + &format!("{:02x}", b);
    }
    r
}

// -------------------------------------------------------------
//  https://github.com/stainless-steel/md5

use std::ops::{Deref, DerefMut};

pub(crate) struct Md5Digest(pub [u8; 16]);

impl Deref for Md5Digest {
    type Target = [u8; 16];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Md5Digest {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Md5Digest> for [u8; 16] {
    #[inline]
    fn from(digest: Md5Digest) -> Self {
        digest.0
    }
}

pub(crate) struct MD5 {
    buffer: [u8; 64],
    count: [u32; 2],
    state: [u32; 4],
}

impl MD5 {
    #[inline]
    pub fn new() -> MD5 {
        MD5 {
            buffer: [0; 64],
            count: [0, 0],
            state: [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476],
        }
    }

    pub fn update<T: AsRef<[u8]>>(&mut self, data: T) {
        for chunk in data.as_ref().chunks(std::u32::MAX as usize) {
            process_chunk(self, chunk);
        }
    }

    pub fn finish(mut self) -> Md5Digest {
        let mut input = [0u32; 16];
        let k = ((self.count[0] >> 3) & 0x3f) as usize;
        let mut padding = [0_u8; 64];
        padding[0] = 0x80;
        input[14] = self.count[0];
        input[15] = self.count[1];
        process_chunk(
            &mut self,
            &padding[..(if k < 56 {
                56 - k
            } else {
                120 - k
            })],
        );
        let mut j = 0;
        for i in 0..14 {
            input[i] = ((self.buffer[j + 3] as u32) << 24) | ((self.buffer[j + 2] as u32) << 16) | ((self.buffer[j + 1] as u32) << 8) | (self.buffer[j] as u32);
            j += 4;
        }
        transform(&mut self.state, &input);
        let mut digest = [0u8; 16];
        let mut j = 0;
        for i in 0..4 {
            digest[j] = ((self.state[i]) & 0xff) as u8;
            digest[j + 1] = ((self.state[i] >> 8) & 0xff) as u8;
            digest[j + 2] = ((self.state[i] >> 16) & 0xff) as u8;
            digest[j + 3] = ((self.state[i] >> 24) & 0xff) as u8;
            j += 4;
        }
        Md5Digest(digest)
    }
}

#[inline]
fn process_chunk(
    MD5 {
        buffer,
        count,
        state,
    }: &mut MD5,
    data: &[u8],
) {
    let mut input = [0u32; 16];
    let mut k = ((count[0] >> 3) & 0x3f) as usize;
    let length = data.len() as u32;
    count[0] = count[0].wrapping_add(length << 3);
    if count[0] < length << 3 {
        count[1] = count[1].wrapping_add(1);
    }
    count[1] = count[1].wrapping_add(length >> 29);
    for &value in data {
        buffer[k] = value;
        k += 1;
        if k == 0x40 {
            let mut j = 0;
            for i in 0..16 {
                input[i] = ((buffer[j + 3] as u32) << 24) | ((buffer[j + 2] as u32) << 16) | ((buffer[j + 1] as u32) << 8) | (buffer[j] as u32);
                j += 4;
            }
            transform(state, &input);
            k = 0;
        }
    }
}

#[inline]
fn transform(state: &mut [u32; 4], input: &[u32; 16]) {
    let (mut a, mut b, mut c, mut d) = (state[0], state[1], state[2], state[3]);
    macro_rules! add(
        ($a:expr, $b:expr) => ($a.wrapping_add($b));
    );
    macro_rules! rotate(
        ($x:expr, $n:expr) => (($x << $n) | ($x >> (32 - $n)));
    );
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => (($x & $y) | (!$x & $z));
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 = 7;
        const S2: u32 = 12;
        const S3: u32 = 17;
        const S4: u32 = 22;
        T!(a, b, c, d, input[0], S1, 3614090360);
        T!(d, a, b, c, input[1], S2, 3905402710);
        T!(c, d, a, b, input[2], S3, 606105819);
        T!(b, c, d, a, input[3], S4, 3250441966);
        T!(a, b, c, d, input[4], S1, 4118548399);
        T!(d, a, b, c, input[5], S2, 1200080426);
        T!(c, d, a, b, input[6], S3, 2821735955);
        T!(b, c, d, a, input[7], S4, 4249261313);
        T!(a, b, c, d, input[8], S1, 1770035416);
        T!(d, a, b, c, input[9], S2, 2336552879);
        T!(c, d, a, b, input[10], S3, 4294925233);
        T!(b, c, d, a, input[11], S4, 2304563134);
        T!(a, b, c, d, input[12], S1, 1804603682);
        T!(d, a, b, c, input[13], S2, 4254626195);
        T!(c, d, a, b, input[14], S3, 2792965006);
        T!(b, c, d, a, input[15], S4, 1236535329);
    }
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => (($x & $z) | ($y & !$z));
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 = 5;
        const S2: u32 = 9;
        const S3: u32 = 14;
        const S4: u32 = 20;
        T!(a, b, c, d, input[1], S1, 4129170786);
        T!(d, a, b, c, input[6], S2, 3225465664);
        T!(c, d, a, b, input[11], S3, 643717713);
        T!(b, c, d, a, input[0], S4, 3921069994);
        T!(a, b, c, d, input[5], S1, 3593408605);
        T!(d, a, b, c, input[10], S2, 38016083);
        T!(c, d, a, b, input[15], S3, 3634488961);
        T!(b, c, d, a, input[4], S4, 3889429448);
        T!(a, b, c, d, input[9], S1, 568446438);
        T!(d, a, b, c, input[14], S2, 3275163606);
        T!(c, d, a, b, input[3], S3, 4107603335);
        T!(b, c, d, a, input[8], S4, 1163531501);
        T!(a, b, c, d, input[13], S1, 2850285829);
        T!(d, a, b, c, input[2], S2, 4243563512);
        T!(c, d, a, b, input[7], S3, 1735328473);
        T!(b, c, d, a, input[12], S4, 2368359562);
    }
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => ($x ^ $y ^ $z);
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 = 4;
        const S2: u32 = 11;
        const S3: u32 = 16;
        const S4: u32 = 23;
        T!(a, b, c, d, input[5], S1, 4294588738);
        T!(d, a, b, c, input[8], S2, 2272392833);
        T!(c, d, a, b, input[11], S3, 1839030562);
        T!(b, c, d, a, input[14], S4, 4259657740);
        T!(a, b, c, d, input[1], S1, 2763975236);
        T!(d, a, b, c, input[4], S2, 1272893353);
        T!(c, d, a, b, input[7], S3, 4139469664);
        T!(b, c, d, a, input[10], S4, 3200236656);
        T!(a, b, c, d, input[13], S1, 681279174);
        T!(d, a, b, c, input[0], S2, 3936430074);
        T!(c, d, a, b, input[3], S3, 3572445317);
        T!(b, c, d, a, input[6], S4, 76029189);
        T!(a, b, c, d, input[9], S1, 3654602809);
        T!(d, a, b, c, input[12], S2, 3873151461);
        T!(c, d, a, b, input[15], S3, 530742520);
        T!(b, c, d, a, input[2], S4, 3299628645);
    }
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => ($y ^ ($x | !$z));
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 = 6;
        const S2: u32 = 10;
        const S3: u32 = 15;
        const S4: u32 = 21;
        T!(a, b, c, d, input[0], S1, 4096336452);
        T!(d, a, b, c, input[7], S2, 1126891415);
        T!(c, d, a, b, input[14], S3, 2878612391);
        T!(b, c, d, a, input[5], S4, 4237533241);
        T!(a, b, c, d, input[12], S1, 1700485571);
        T!(d, a, b, c, input[3], S2, 2399980690);
        T!(c, d, a, b, input[10], S3, 4293915773);
        T!(b, c, d, a, input[1], S4, 2240044497);
        T!(a, b, c, d, input[8], S1, 1873313359);
        T!(d, a, b, c, input[15], S2, 4264355552);
        T!(c, d, a, b, input[6], S3, 2734768916);
        T!(b, c, d, a, input[13], S4, 1309151649);
        T!(a, b, c, d, input[4], S1, 4149444226);
        T!(d, a, b, c, input[11], S2, 3174756917);
        T!(c, d, a, b, input[2], S3, 718787259);
        T!(b, c, d, a, input[9], S4, 3951481745);
    }
    state[0] = add!(state[0], a);
    state[1] = add!(state[1], b);
    state[2] = add!(state[2], c);
    state[3] = add!(state[3], d);
}

// -----------------------------
//  https://github.com/sfackler/rust-postgres/blob/master/postgres-protocol/src/authentication/sasl.rs

use hmac::{Hmac, Mac};
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io::{self, Error as ioError, ErrorKind};
use std::mem::replace as memreplace;

fn normalize(pass: &[u8]) -> Vec<u8> {
    let pass = match std::str::from_utf8(pass) {
        Ok(v) => v,
        Err(_) => return pass.to_vec(),
    };

    match stringprep::saslprep(pass) {
        Ok(pass) => pass.into_owned().into_bytes(),
        Err(_) => pass.as_bytes().to_vec(),
    }
}

enum ScramState {
    Update {
        nonce: String, //  length 24
        password: Vec<u8>,
    },
    Finish {
        salted_password: [u8; 32],
        auth_message: String,
    },
    Done,
}

struct ScramSha256 {
    buffer: String,
    state: ScramState,
}

impl ScramSha256 {
    fn new(password: &[u8]) -> ScramSha256 {
        let buffer = String::from("");
        let nonce = rand_buf24();
        ScramSha256 {
            buffer: buffer,
            state: ScramState::Update {
                nonce,
                password: normalize(password),
            },
        }
    }

    fn genhashdata(&self) -> Vec<u8> {
        let mut databuf: Vec<u8> = vec![];
        match &self.state {
            ScramState::Update {
                nonce,
                password: _,
            } => {
                let scrypto = "SCRAM-SHA-256\x00";
                let sclient = "n,,n=,r=";
                let dlen = sclient.len() + nonce.len();
                databuf.extend_from_slice(scrypto.as_bytes());
                let plen = put_i32(dlen as i32);
                databuf.extend_from_slice(&plen);
                databuf.extend_from_slice(sclient.as_bytes());
                databuf.extend_from_slice(nonce.as_bytes());
            }
            ScramState::Finish {
                salted_password: _,
                auth_message: _,
            } => {
                databuf.extend_from_slice(self.buffer.as_bytes());
            }
            ScramState::Done => {}
        }
        return databuf;
    }

    fn update(&mut self, message: &[u8]) -> io::Result<()> {
        let (client_nonce, password) = match memreplace(&mut self.state, ScramState::Done) {
            ScramState::Update {
                nonce,
                password,
            } => (nonce, password),
            _ => return Err(ioError::new(ErrorKind::Other, "invalid SCRAM state")),
        };

        let message = std::str::from_utf8(message).map_err(|e| ioError::new(ErrorKind::InvalidInput, e))?;

        let mut nonce = String::from("");
        let mut psalt = String::from("");
        let mut iterations = String::from("");
        let parts = message.split(",");
        let items: Vec<&str> = parts.collect();
        let kvlen = items.len();
        for i in 0..kvlen {
            let kv = items[i];
            if (&kv[0..1] == "r") && (kv.len() > 2) {
                nonce = kv[2..].to_string();
            }
            if (&kv[0..1] == "s") && (kv.len() > 2) {
                psalt = kv[2..].to_string();
            }
            if (&kv[0..1] == "i") && (kv.len() > 2) {
                iterations = kv[2..].to_string();
            }
        }

        if (nonce.len() == 0) || (psalt.len() == 0) || (iterations.len() == 0) {
            return Err(ioError::new(ErrorKind::InvalidInput, "server_first_message length is 0"));
        }

        if !nonce.starts_with(&client_nonce) {
            return Err(ioError::new(ErrorKind::InvalidInput, "invalid nonce"));
        }

        let salt = b64decode(psalt.into());
        let iteration_count = match iterations.parse::<u32>() {
            Ok(v) => v,
            Err(_) => 0,
        };

        let salted_password = hi(&password, &salt, iteration_count);

        let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password).expect("HMAC is able to accept all key sizes");
        hmac.update(b"Client Key");
        let client_key = hmac.finalize().into_bytes();

        let mut hash = Sha256::default();
        hash.update(client_key.as_slice());
        let stored_key = hash.finalize_fixed();

        let cbind_input = "biws"; //  base64 of 'n,,'

        self.buffer.clear();
        write!(&mut self.buffer, "c={},r={}", cbind_input, nonce).unwrap();

        let auth_message = format!("n=,r={},{},{}", client_nonce, message, self.buffer);

        let mut hmac = Hmac::<Sha256>::new_from_slice(&stored_key).expect("HMAC is able to accept all key sizes");
        hmac.update(auth_message.as_bytes());
        let client_signature = hmac.finalize().into_bytes();

        let mut client_proof = client_key;
        for (proof, signature) in client_proof.iter_mut().zip(client_signature) {
            *proof ^= signature;
        }

        write!(&mut self.buffer, ",p={}", b64encode(&client_proof)).unwrap();
        self.state = ScramState::Finish {
            salted_password,
            auth_message,
        };
        Ok(())
    }

    fn finish(&mut self, message: &[u8]) -> io::Result<()> {
        let (salted_password, auth_message) = match memreplace(&mut self.state, ScramState::Done) {
            ScramState::Finish {
                salted_password,
                auth_message,
            } => (salted_password, auth_message),
            _ => return Err(ioError::new(ErrorKind::Other, "invalid SCRAM state")),
        };

        let message = std::str::from_utf8(message).map_err(|e| ioError::new(ErrorKind::InvalidInput, e))?;

        if (&message[0..1] != "v") && (message.len() <= 2) {
            return Err(ioError::new(ErrorKind::InvalidInput, "server_final_message length is 0"));
        }

        let verifier = message[2..].to_string();

        let verifier = b64decode(verifier.into());

        let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password).expect("HMAC is able to accept all key sizes");
        hmac.update(b"Server Key");
        let server_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::<Sha256>::new_from_slice(&server_key).expect("HMAC is able to accept all key sizes");
        hmac.update(auth_message.as_bytes());
        hmac.verify_slice(&verifier).map_err(|_| ioError::new(ErrorKind::InvalidInput, "SCRAM verification error"))
    }
}

fn hi(str: &[u8], salt: &[u8], i: u32) -> [u8; 32] {
    let mut hmac = Hmac::<Sha256>::new_from_slice(str).expect("HMAC is able to accept all key sizes");
    hmac.update(salt);
    hmac.update(&[0, 0, 0, 1]);
    let mut prev = hmac.finalize().into_bytes();
    let mut hi = prev;
    for _ in 1..i {
        let mut hmac = Hmac::<Sha256>::new_from_slice(str).expect("already checked above");
        hmac.update(&prev);
        prev = hmac.finalize().into_bytes();

        for (hi, prev) in hi.iter_mut().zip(prev) {
            *hi ^= prev;
        }
    }

    hi.into()
}

// -------------------------------------------------------------

#[inline]
fn rand_buf24() -> String {
    use std::{fs::File, io::Read};
    let f = File::open("/dev/urandom"); //  "/dev/random"
    let mut buffer = [0x3fu8; 1024];
    let mut res = String::new();
    match f {
        Ok(f) => {
            let mut f = &f;
            f.read(&mut buffer).unwrap();
        }
        Err(_) => {
            //  buffer = buffer.iter().map(|x| x + 1).collect::<Vec<_>>().try_into();
            for i in 0..buffer.len() {
                buffer[i] = (((buffer[i] as usize * (i + 13)) + ((&i as *const usize) as usize & 0xffff) * (i + 7) + (i + 29) * 23) & 0xff) as u8;
            }
        }
    }
    for c in buffer {
        match c {
            0x21..=0x2b => res.push(c as char),
            0x2c => res.push(0x7e as char),
            0x2d..=0x7e => res.push(c as char),
            _ => {}
        }
        if res.len() == 24 {
            break;
        }
    }
    res
}

#[inline]
fn b64decode(bytes: Vec<u8>) -> Vec<u8> {
    let mut b64r: Vec<u8> = vec![];
    let length = bytes.len();
    let mut i = 0;
    let mut b: u8;
    while i < length {
        let chr1: u8 = b64c2idx(bytes[i]);
        let chr2: u8 = b64c2idx(bytes[i + 1]);
        b = b64c2idx(bytes[i + 2]);
        let chr3: u8 = if b != 0x40 {
            b
        } else {
            0xff
        };
        b = b64c2idx(bytes[i + 3]);
        let chr4: u8 = if b != 0x40 {
            b
        } else {
            0xff
        };
        i += 4;
        b64r.push((chr1 << 2) | (chr2 >> 4));
        if chr3 != 0xff {
            b64r.push((chr2 << 4) | (chr3 >> 2))
        } else {
            i = length
        };
        if chr4 != 0xff {
            b64r.push((chr3 << 6) | chr4)
        } else {
            i = length
        };
    }
    b64r
}

#[inline]
fn b64c2idx(b: u8) -> u8 {
    match b {
        0x2b => return 0x3e,            //  +
        0x2f => return 0x3f,            //  /
        0x30..=0x39 => return b + 0x04, //  A-Z
        0x41..=0x5a => return b - 0x41, //  a-z
        0x61..=0x7a => return b - 0x47, //  0-9
        _ => return 0x40,               //  = 0x3d
    }
}

#[inline]
fn b64encode(s: &[u8]) -> String {
    let mut b64r = String::new();
    let length = s.len();

    let mut i = 0;

    while i < length {
        let b1: u8 = s[i];
        i += 1;

        let b2: usize = if i < length {
            s[i] as usize
        } else {
            256
        };
        i += 1;

        let b3: usize = if i < length {
            s[i] as usize
        } else {
            256
        };
        i += 1;

        let chr1: u8 = b1 >> 2;
        let chr2: u8 = ((b1 & 3) << 4) | ((b2 as u8) >> 4);

        let mut chr3: u8 = (((b2 as u8) & 15) << 2) | ((b3 as u8) >> 6);
        let mut chr4: u8 = (b3 as u8) & 63;

        if b2 == 256 {
            chr3 = 64;
            chr4 = 64;
        } else if b3 == 256 {
            chr4 = 64;
        }

        b64r.push(idx2b64c(chr1));
        b64r.push(idx2b64c(chr2));
        b64r.push(idx2b64c(chr3));
        b64r.push(idx2b64c(chr4));
    }
    b64r
}

#[inline]
fn idx2b64c(b: u8) -> char {
    match b {
        0x00..=0x19 => return (b + 0x41) as char, //  A-Z
        0x1a..=0x33 => return (b + 0x47) as char, //  a-z
        0x34..=0x3d => return (b - 0x04) as char, //  0-9
        0x3e => return 0x2b as char,              //  +
        0x3f => return 0x2f as char,              //  /
        _ => return 0x3d as char,                 //  0x3d =
    }
}

/*

// backend
const PARSECOMPLETE: u8 = b'1';
const BINDCOMPLETE: u8 = b'2';
const CLOSECOMPLETE: u8 = b'3';
const PARAMETERDESCRIPTION: u8 = b't';
const FUNCTIONCALLRESPONSE: u8 = b'V';
const PORTALSUSPENDED: u8 = b's';
const NODATA: u8 = b'n';

// frontend
const PARSE: u8 = b'P';
const BIND: u8 = b'B';
const DESCRIBE: u8 = b'D';
const FUNCTIONCALL: u8 = b'F';
const EXECUTE: u8 = b'E';
const SYNC: u8 = b'S';
const FLUSH: u8 = b'H';
const CLOSE: u8 = b'C';

 Error returned from Postgres server
 'S' => "SeverityS",
 'V' => "Severity",
 'C' => "Code",
 'M' => "Message",
 'D' => "Detail",
 'H' => "Hint",
 'P' => "Position",
 'p' => "Internal position",
 'q' => "Internal query",
 'W' => "Where",
 's' => "Schema name",
 't' => "Table name",
 'c' => "column name",
 'd' => "Data type name",
 'n' => "Constraint name",
 'F' => "File",
 'L' => "Line",
 'R' => "Routine",
*/
