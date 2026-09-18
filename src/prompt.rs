use serde::Deserialize;
use serde::Serialize;
use std::io::Write;
use uuid::Uuid;

pub enum Intent {
    Login,
    Debug,
}

pub fn prompt_login(intent: Intent) -> anyhow::Result<String> {
    // TODO: Parameterize url
    let url = "https://papaya-repeal-curvature.ngrok-free.dev/personal_access_tokens";

    match intent {
        Intent::Login => {
            println!("To login, enter your personal access token.");
        }
        Intent::Debug => {
            println!("Before starting your debug session, enter your personal access token.");
        }
    }

    println!("\nYou can generate a personal access token at {url}\n");

    println!("Copy your personal access token, paste it here, and hit ENTER:");
    print!("Token: ");
    std::io::stdout().flush()?;

    let secret = rpassword::read_password()?;

    Ok(secret)
}

#[derive(Serialize, Deserialize)]
pub struct AccountsResponse {
    pub accounts: Vec<Account>,
}

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub account_id: Uuid,
    pub name: String,
}

pub async fn prompt_account_selection(secret: &str) -> anyhow::Result<Uuid> {
    let url = "https://papaya-repeal-curvature.ngrok-free.dev/cli_accounts";
    let response = reqwest::Client::new()
        .get(url)
        .header("x-muzanci-personal-access-token", secret)
        .header("accept", "application/json")
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await?;
        anyhow::bail!("failed to fetch accounts: {body}");
    }

    let accounts = response.json::<AccountsResponse>().await?.accounts;

    if accounts.is_empty() {
        anyhow::bail!("no accounts found");
    }

    if accounts.len() == 1 {
        return Ok(accounts[0].account_id);
    }

    println!("Your MuzanCI profile is associated with multiple MuzanCI accounts.");
    println!("Which MuzanCI account would you like to use?:");

    for (i, account) in accounts.iter().enumerate() {
        println!("{}: {} ({})", i + 1, account.name, account.account_id);
    }

    print!("Enter your choice: ");
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let choice: usize = input.trim().parse()?;

    if choice < 1 || choice > accounts.len() {
        anyhow::bail!("invalid choice: {choice}");
    }

    let account_id = accounts[choice - 1].account_id;

    Ok(account_id)
}
