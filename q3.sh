set -e
RESET_CACHE=false
EXECUTE=false
PLAN_FOLDER="./executeResults"
N=20
SF=0.1
EXEC="cargo run -p outputer -- "
QUERY_FILE="./sqlFiles/q3.params.sql"
YML_TEMPLATE="./sqlFiles/q3.params.yml"
TEST_FOLDER="."



# ======================================
# Parse de argumentos
# ======================================

while [[ $# -gt 0 ]]; do
  case $1 in
    --reset)
      RESET_CACHE=true
      shift
      ;;
    --execute)
      EXECUTE=true
      shift
      ;;
    --plans)
      PLAN_FOLDER="$2"
      shift 2
      ;;
    --n)
      N="$2"
      shift 2
      ;;
    --sf)
      SF="$2"
      shift 2
      ;;
    *)
      echo "Argumento desconhecido: $1"
      exit 1
      ;;
  esac
done

# ======================================
# Pasta de execução
# ======================================

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_FOLDER="${PLAN_FOLDER}/${TIMESTAMP}"
mkdir -p "$RUN_FOLDER"

echo "RESET_CACHE = $RESET_CACHE"
echo "EXECUTE     = $EXECUTE"
echo "N           = $N"
echo "RUN_FOLDER  = $RUN_FOLDER"
echo

# ======================================
# Valores em [0,1]
# ======================================

VALUES=()
for ((i=0; i<N; i++)); do
  VALUES+=("$(awk -v i="$i" -v n="$N" 'BEGIN { printf "%.2f", (i + 0.5)/n }')")
done

# ======================================
# Argumentos configuráveis
# ======================================

#ARGS_DEFAULT=""
#ARGS_EXECUTE="-f populate.sql"

#ARGS="$ARGS_DEFAULT"
#if $EXECUTE; then
#  ARGS="$ARGS_EXECUTE"
#fi

# ======================================
# Compilar uma vez
# ======================================

cargo build -p optd-cli 

# ======================================
# Experimentos
# ======================================

for x0 in "${VALUES[@]}"; do
  for x1 in "${VALUES[@]}"; do

    echo "--------------------------------------"
    echo "x0=$x0 | x1=$x1"
    echo "--------------------------------------"

    
    scaledX1=$(awk -v "x1=$x0" -v "sf=$SF" 'BEGIN { printf "%.0f", x1 * sf * 150000}')
    scaledX2=$(awk -v "x2=$x1" -v "sf=$SF" 'BEGIN { printf "%.0f", x2 * sf * 1500000}')
    
    echo "Scaled values: x1=$scaledX1 | x2=$scaledX2"
    
    TEST_NAME="tmp_query_X0_${x0}_X1_${x1}"
    TMP_QUERY_FILE="${TEST_FOLDER}/${TEST_NAME}.planner.sql"

    sed "s/\\\$1/${scaledX1}/g; s/\\\$2/${scaledX2}/g" "$QUERY_FILE" > "$TMP_QUERY_FILE"
    
    FILE="$RUN_FOLDER/plan_X0_${x0}_X1_${x1}.txt"

    $EXEC -f $TMP_QUERY_FILE -p populate.sql > "$FILE"

    rm "$TMP_QUERY_FILE"

  done
done

echo "✔ Execução concluída"