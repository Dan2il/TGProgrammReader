
// mod server;
use zbus::Connection;
use zbus::Proxy;
use futures_util::stream::StreamExt;
use serde_json::json;
use tdlib::{
    enums::{AuthorizationState, Update, User},
    functions,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::io::{self, Write};
use dotenvy::dotenv;
use std::env;


fn ask_user(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

async fn handle_update(update: Update, auth_tx: &Sender<AuthorizationState>) {
    // println!("Received update: {:?}", update);
    if let Update::AuthorizationState(update) = update {
        auth_tx.send(update.authorization_state).await.unwrap();
    }
}

async fn handle_authorization_state(
    client_id: i32,
    mut auth_rx: Receiver<AuthorizationState>,
    run_flag: Arc<AtomicBool>,
) -> Receiver<AuthorizationState> {
    let api_id: i32 = env::var("API_ID").expect("ERROR INIT API ID").parse().expect("Error parse API ID");
    let api_hash: String = env::var("API_HASH").expect("ERROR INIT API HASH");

    while let Some(state) = auth_rx.recv().await {
        match state {
            AuthorizationState::WaitTdlibParameters => {
                println!("WaitTdlibParameters");
                let _ = functions::set_tdlib_parameters(
                    false,
                    "tdlib_data".into(),
                    "".into(),
                    "".into(),
                    false,
                    false,
                    false,
                    false,
                    api_id,
                    api_hash.clone(),
                    "en".into(),
                    "RustTDLib".into(),
                    "".into(),
                    env!("CARGO_PKG_VERSION").into(),
                    false,
                    true,
                    client_id,
                )
                .await;
            }
            AuthorizationState::WaitPhoneNumber => {
                println!("WaitPhoneNumber");
                loop {
                    let phone = ask_user("Enter your phone number (+7xxx): ");
                    if functions::set_authentication_phone_number(phone, None, client_id)
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
            }
            AuthorizationState::WaitCode(_) => {
                println!("WaitCode");
                loop {
                    let code = ask_user("Enter code: ");
                    match functions::check_authentication_code(code, client_id).await {
                        Ok(_) => {
                            println!("Code accepted, authorization in progress...");
                            break;
                        }
                        Err(e) => {
                            println!("Failed to check code: {}", e.message);
                        }
                    }
                }
            }
            AuthorizationState::WaitPassword(_) => {
                println!("WaitPassword");
                loop {
                    let password = ask_user("Enter 2FA password: ");
                    match functions::check_authentication_password(password, client_id).await {
                        Ok(_) => {
                            println!("Password accepted, authorization in progress...");
                            break;
                        }
                        Err(e) => {
                            println!("Failed to check password: {}", e.message);
                        }
                    }
                }
            }
            AuthorizationState::Ready => {
                println!("Authorization complete!");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                break;
            }
            AuthorizationState::Closed => {
                println!("Closed");
                run_flag.store(false, Ordering::Release);
                break;
            }
            _ => {}
        }
    }

    auth_rx
}


#[tokio::main]
async fn main() -> zbus::Result<()> {
    dotenv().ok();

    let client_id = tdlib::create_client();
    let (auth_tx, auth_rx) = mpsc::channel(5);
    let run_flag = Arc::new(AtomicBool::new(true));
    let run_flag_clone = run_flag.clone();

    // Фоновая задача для получения обновлений TDLib
    tokio::spawn(async move {
        while run_flag_clone.load(Ordering::Acquire) {
            if let Some((update, _)) = tdlib::receive() {
                handle_update(update, &auth_tx).await;
            }
        }
    });

    functions::set_log_verbosity_level(2, client_id).await.unwrap();
    println!("set_log_verbosity_level");
    let _ = handle_authorization_state(client_id, auth_rx, run_flag.clone()).await;;
    println!("Create tg client");
    let conn = Connection::session().await?;

    let pr = Proxy::new(
        &conn,
        "org.example.TGService",
        "/org/example/TGService",
        "org.example.TGService",
    ).await?;

    let counter: u32 = pr.call("GetValue", &()).await?;
    let window: String = pr.call("GetWindow", &()).await?;
    println!("Initial state: counter={} window={}", counter, window);

    let mut cntr_sig = pr.receive_signal(
        "CounterUpdated",
    ).await?;

    let mut win_sgnl = pr.receive_signal(
        "WindowUpdated",
    ).await?;

    println!("Create resive for signals");

    loop {
        tokio::select! {
            Some(signal) = cntr_sig.next() => {
                let body = signal.body();
                match body.deserialize::<(u32,)>() {
                    Ok((val,)) => println!("Counter updated: {}", val),
                    Err(err) => eprintln!("Failed to parse counter signal: {}", err),
                }
            }

            Some(signal) = win_sgnl.next() => {
                let body = signal.body();
                match body.deserialize::<(String,)>() {
                    Ok((name,)) => {
                        println!("Active window changed: {}", name);
                        let bio_text = format!("Currently using: {}", name);
                        if let Err(e) = functions::set_bio(bio_text, client_id).await {
                            eprintln!("Failed to update bio: {}", e.message);
                        }
                    }
                    Err(err) => eprintln!("Failed to parse window signal: {}", err),
                }
            }
        }
    }
}
