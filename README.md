# optd

Query Optimizer Service.


## Get Started

To interact with the CLI, run

```bash
cargo run -p optd-cli
```




## Structure

- `optd/core`: The core optimizer implementation (IR, properties, rules, cost model, cardinality estimation).
- `optd/catalog`: A persistent catalog implementation.
- `connectors/datafusion`: Utilities needed to use optd in DataFusion.
- `cli`: command line interface based on [`datafusion-cli`](https://datafusion.apache.org/user-guide/cli/index.html).
- `sqlFiles`: 





### Non presistent

cargo run -p outputer -- -f .\sqlFiles\q3.example.sql   -p  .\sqlFiles\populate.sql > .\outputs\nonpresistent.txt 

### Presistent

cargo run -p outputer -- -f .\sqlFiles\q3.example.sql -m  -p  .\sqlFiles\populate.sql .\sqlFiles\memo.sql .\sqlFiles\memopopulate.sql > .\outputs\presistent.txt 


### NOTAS

já está funcional a presistência mas penso que vou ter que extender os testes para mais querys diferentes, algum scalar/operador pode não ter a conversão correta ainda
Acrescentar um lookup do GroupId para o plano, neste momento ainda está hardcoded
De momento tempo sem presistencia aproximadamente igual ao tempo com presistencia



### Passos seguintes

Ver a simplificaçáo para os testes do professor, utilizar source e output columns para as colunas de output/input.




## Proxima semana

Apresentar alterações que efetuei e suas limitações

Serde Json  https://docs.rs/serde_json/latest/serde_json/



(Get-Content .\data\group.json -Raw | ConvertFrom-Json) | ForEach-Object { $_ | ConvertTo-Json -Compress } | Set-Content .\data\group.ndjson
(Get-Content .\data\expression.json -Raw | ConvertFrom-Json) | ForEach-Object { $_ | ConvertTo-Json -Compress } | Set-Content .\data\expression.ndjson
(Get-Content .\data\scalar.json -Raw | ConvertFrom-Json) | ForEach-Object { $_ | ConvertTo-Json -Compress } | Set-Content .\data\scalar.ndjson
(Get-Content .\data\expressioninput.json -Raw | ConvertFrom-Json) | ForEach-Object { $_ | ConvertTo-Json -Compress } | Set-Content .\data\expressioninput.ndjson
(Get-Content .\data\expressionscalar.json -Raw | ConvertFrom-Json) | ForEach-Object { $_ | ConvertTo-Json -Compress } | Set-Content .\data\expressionscalar.ndjson