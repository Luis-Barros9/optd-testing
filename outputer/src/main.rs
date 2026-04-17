use std::fs;
use std::collections::HashMap;
use std::time::Instant;
use anyhow::{bail, Result};
use clap::Parser;
use datafusion::arrow::{array::RecordBatch, util::display::{ArrayFormatter, FormatOptions}};
use optd_datafusion::DataFusionDB;

#[derive(Parser)]
#[command(name = "SQL Query Explainer")]
#[command(about = "Executa EXPLAIN VERBOSE em queries SQL", long_about = None)]
struct Cli {
    /// Caminho(s) do(s) ficheiro(s) com queries SQL (executados pela ordem fornecida)
    #[arg(short = 'f', long, value_name = "FILE", num_args = 1..)]
    query_files: Vec<String>,

    /// Query SQL direta (alternativa ao ficheiro)
    #[arg(short = 'q', long)]
    query: Option<String>,

    /// Mostrar apenas o plano sem detalhes
    #[arg(short = 's', long)]
    simple: bool,

    /// Ficheiro(s) SQL para popular a base de dados antes da query
    #[arg(short = 'p', long, value_name = "FILE", num_args = 1..)]
    populate: Vec<String>,

    /// Ativar modo de persistência do memo
    #[arg(short = 'm', long, default_value_t = false)]
    memo_presist: bool,

    /// Modo teste: executar query N vezes (carrega dados uma única vez). Default: 1 se fornecido sem valor
    #[arg(short = 't', long, num_args = 0..=1, default_missing_value = "1", value_parser = clap::value_parser!(u32))]
    test: Option<u32>,

    
}


pub async fn get_memo_from_db(db: &DataFusionDB) -> Result<HashMap<String, Vec<RecordBatch>>> {
    // possivelmente alterar para não usar strings e usar o record batch
    let statements = [
        ("group", "SELECT * FROM group "),
        ("expression", "SELECT * FROM expression"),
        ("expression_input", "SELECT * FROM expression_input"),
        ("scalar", "SELECT * FROM scalar ORDER BY id DESC"),
        ("expression_scalar", "SELECT * FROM expression_scalar"),
    ];

    let mut memo_rows  = HashMap::new();
    for (table_name, statement) in statements {
        let rows = db.execute(statement).await?;
        memo_rows.insert(table_name.to_string(), rows);
    }

    Ok(memo_rows)
}

pub async fn execute(db: &DataFusionDB, sql: &str) -> Result<Vec<Vec<String>>> {
    let batches = db.execute(sql).await?;
    let options = FormatOptions::default().with_null("NULL");
    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        let converters = batch
            .columns()
            .iter()
            .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for converter in converters.iter() {
                let mut buffer = String::with_capacity(8);
                converter.value(row_idx).write(&mut buffer)?;
                row.push(buffer);
            }
            result.push(row);
        }
    }
    Ok(result)
}

/*
#[async_trait::async_trait]
impl sqlplannertest::PlannerTestRunner for PlannerTestDB {
    async fn run(&mut self, test_case: &sqlplannertest::ParsedTestCase) -> Result<String> {
        use itertools::Itertools;
        use std::fmt::Write;

        let mut result = String::new();
        let r = &mut result;
        for sql in &test_case.before_sql {
            // We drop output of before statements
            self.0.execute(sql).await?;
        }

        for task_str in &test_case.tasks {
            let task = parse_task(task_str)?;
            match task {
                PlannerTestTask::Execute => {
                    let result = self.execute(&test_case.sql).await?;
                    writeln!(r, "{}", result.into_iter().map(|x| x.join(" ")).join("\n"))?;
                    writeln!(r)?;
                }
                PlannerTestTask::Explain => {
                    // Handle the Explain task here
                    let explained_sql = format!("EXPLAIN verbose {}", test_case.sql);
                    let result = self.execute(&explained_sql).await?;
                    let explained_output = result
                        .into_iter()
                        .filter_map(|row| match row[0].as_str() {
                            "logical_plan after optd-initial"
                            | "physical_plan after optd-finalized" => Some(row[0..2].join(":\n")),
                            _ => None,
                        })
                        .join("\n\n");
                    writeln!(r, "{}\n", explained_output)?;
                }
            }
        }

        Ok(result)
    }
}*/


#[tokio::main]
async fn main() -> Result<()> {

    // permitir logs de debug
    //$env:RUST_LOG="info"
    //$env:RUST_LOG="debug"
    //$env:RUST_LOG="optd=info,outputer=debug"
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    if cli.query.is_some() && !cli.query_files.is_empty() {
        bail!("Use apenas um modo: --query ou --file/--f");
    }

    if cli.query.is_none() && cli.query_files.is_empty() {
        bail!("Indique pelo menos uma query com --query ou um/mais ficheiros com --file/--f");
    }

    // Ler queries: uma query direta ou varias queries de ficheiro pela ordem fornecida
    let sql_queries = if let Some(q) = cli.query {
        vec![("<inline-query>".to_string(), q)]
    } else {
        cli.query_files
            .iter()
            .map(|file| Ok((file.clone(), fs::read_to_string(file)?)))
            .collect::<Result<Vec<_>>>()?
    };

    //println!("📋 Query: {}\n", sql_query);

    // Criar conexão com DataFusionDB
    let db = optd_datafusion::DataFusionDB::new().await?;

    // Popular a base de dados se ficheiro(s) foram fornecidos
    for populate_file in &cli.populate {
        let populate_sql = fs::read_to_string(populate_file)?;
                    
        // Dividir por `;` e executar cada statement
        let statements: Vec<&str> = populate_sql
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        for (idx, stmt) in statements.iter().enumerate() {
            match db.execute(stmt).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!(
                        "❌ Erro ao executar statement {} do ficheiro {}: {}",
                        idx + 1,
                        populate_file,
                        e
                    );
                    return Err(e.into());
                }
            }
        }
    }
    


    // Construir comando EXPLAIN para cada query
    
    use itertools::Itertools;
    use std::fmt::Write;

    let mut result = String::new();
    let r = &mut result;

    // TODO : remover, apenas para debug
    
    if cli.memo_presist {
        println!("Preloading memo from DB...");

        let memo_rows = if cli.test.is_some() {
            let start = Instant::now();
            let rows = get_memo_from_db(&db).await?;
            println!("Tempo a ler o memo da DB: {:?}", start.elapsed());
            rows
        } else {
            get_memo_from_db(&db).await?
        };

        db.set_memo_preload_rows(memo_rows);
        db.set_persistent_memo(true);
    }


    for (idx, (source, sql_query)) in sql_queries.iter().enumerate() {
        let explained_sql = if cli.simple {
            format!("EXPLAIN {}", sql_query)
        } else {
            format!("EXPLAIN VERBOSE {}", sql_query)
        };

        let iterations = cli.test.unwrap_or(1);
        
        if cli.test.is_some() {
            println!("🧪 Modo teste: executando query {} vezes", iterations);
        }
        
        let mut total_duration = std::time::Duration::new(0, 0);
        
        for iteration in 1..=iterations {
            if iterations > 1 {
                println!("Execução {} de {}", iteration, iterations);
            }
            
            let start = Instant::now();
            let result = execute(&db, &explained_sql).await?;
            let elapsed = start.elapsed();
            total_duration += elapsed;
            
            if iterations > 1 {
                println!("Tempo desta execução: {:?}", elapsed);
            }

            let explained_output = result
                .into_iter()
                .filter_map(|row| match row[0].as_str() {
                    "logical_plan after optd-initial"
                    | "physical_plan after optd-finalized" => Some(row[0..2].join(":\n")),
                    _ => None,
                })
                .join("\n\n");

            if iteration == 1 {
                // Mostrar output da primeira execução
                writeln!(r, "{}\n", explained_output)?;
                if sql_queries.len() > 1 {
                    println!("----- [{}] {} -----", idx + 1, source);
                }
                println!("{}", explained_output);
            }
        }
        
        if cli.test.is_some() && iterations > 1 {
            let average_duration = total_duration / iterations as u32;
            println!("\n📊 Estatísticas:");
            println!("  Tempo total: {:?}", total_duration);
            println!("  Tempo médio: {:?}", average_duration);
        }
    }
    /* 
    match db.execute(&explain_query).await {
        Ok(results) => {
            for row in results {
                println!("{:?}", row);
            }
        }
        Err(e) => {
            eprintln!("❌ Erro: {}", e);
            return Err(e.into());
        }
    }*/

    Ok(())
}