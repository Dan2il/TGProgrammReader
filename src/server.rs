use tokio::time;
use zbus::interface;
use zbus::object_server::SignalEmitter;
use zbus::Connection;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use x11rb::connection::Connection as XConn;
use x11rb::protocol::xproto::*;
use x11rb::rust_connection::RustConnection;


#[derive(Clone)]
struct TGService {
    counter: Arc<Mutex<u32>>,
    active_window: Arc<Mutex<String>>,
}

#[interface(name = "org.example.TGService")]
impl TGService {
    async fn get_value(&self) -> u32 {
        *self.counter.lock().await
    }

    async fn get_window(&self) -> String {
        self.active_window.lock().await.clone()
    }

    #[zbus(signal)]
    async fn counter_updated(
        ctx: &SignalEmitter<'_>, new_val: u32,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    async  fn window_updated(
        ctx: &SignalEmitter<'_>, new_val: String,
    ) -> zbus::Result<()>;
}

fn get_active_window_program(conn: &RustConnection) -> Option<String> {
    let screen = &conn.setup().roots[0];
    let root = screen.root;

    let atom_net_active_window = conn.intern_atom(false, b"_NET_ACTIVE_WINDOW").ok()?.reply().ok()?;
    let prop = conn.get_property(false, root, atom_net_active_window.atom, AtomEnum::WINDOW, 0, 1)
        .ok()?.reply().ok()?;
    if prop.value.len() < 4 { return None; }

    let window_id = u32::from_ne_bytes([prop.value[0], prop.value[1], prop.value[2], prop.value[3]]);

    let atom_net_wm_pid = conn.intern_atom(false, b"_NET_WM_PID").ok()?.reply().ok()?;
    let pid_prop = conn.get_property(false, window_id, atom_net_wm_pid.atom, AtomEnum::CARDINAL, 0, 1)
        .ok()?.reply().ok()?;
    if pid_prop.value.len() < 4 { return None; }

    let pid = u32::from_ne_bytes([pid_prop.value[0], pid_prop.value[1], pid_prop.value[2], pid_prop.value[3]]);

    std::fs::read_to_string(format!("/proc/{}/comm", pid)).ok().map(|s| s.trim().to_string())
}




#[tokio::main]
async fn main() -> zbus::Result<()> {
    let conn = Connection::session().await?;

    conn.request_name("org.example.TGService").await?;

    let service = TGService{
        counter: Arc::new(Mutex::new(0)),
        active_window: Arc::new(Mutex::new(String::new())),
    };

    conn.object_server().at("/org/example/TGService", service.clone()).await?;
    let inter  = conn.object_server()
        .interface::<_, TGService>("/org/example/TGService")
        .await
        .expect("Error get interface");

    println!("Service started");

    let (xconn, _) = RustConnection::connect(None).expect("Cannot connect to X11");

    loop {
        let _ = time::sleep(Duration::from_secs(2)).await;

        let mut counter = service.counter.lock().await;
        *counter += 1;

        let em  = inter.signal_emitter();
        println!("Send signal: {}", *counter);
        TGService::counter_updated(em, *counter).await?;

        if let Some(name) = get_active_window_program(&xconn) {
            println!("{:#?}", name);
            let mut active_window = service.active_window.lock().await;
            if *active_window != name {
                *active_window = name.clone();
                println!("Active window changed: {}", name);
                TGService::window_updated(&em, name).await?;
            }
        }

    }
}