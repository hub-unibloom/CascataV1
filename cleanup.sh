#!/usr/bin/env bash
# =============================================================================
# CASCATA V1 — SCRIPT DE LIMPEZA (ALMALINUX 9 EXCLUSIVE)
# =============================================================================
# Remove containers, volumes, imagens e artefatos do Cascata.
# Modos de limpeza (escolha interativa):
#
#   1. Leve    — Para containers, remove containers parados
#   2. Média   — Leve + remove volumes (⚠️ apaga dados!)
#   3. Total   — Média + remove imagens Docker do Cascata
#   4. Nuclear — Total + remove Go, Rust, Node.js instalados pelo Cascata
#
# Uso:
#   chmod +x cleanup.sh
#   .             # modo interativo
#   ./cleanup.sh --soft   # modo leve (sem confirmação)
#   ./cleanup.sh --medium # modo médio (sem confirmação)
#   ./cleanup.sh --full   # modo total (sem confirmação)
#   ./cleanup.sh --nuke   # modo nuclear (sem confirmação)
# =============================================================================

set -euo pipefail

# =============================================================================
# CORES E LOGS
# =============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_ok()   { echo -e "  ${GREEN}✔${NC} $1"; }
log_warn() { echo -e "  ${YELLOW}⚠${NC} $1"; }
log_fail() { echo -e "  ${RED}✖${NC} $1"; }
log_info() { echo -e "  ${BLUE}→${NC} $1"; }

log_header() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# =============================================================================
# BANNER
# =============================================================================
echo ""
echo -e "${RED}"
echo "   ██████╗ ██╗     ███████╗ █████╗ ███╗   ██╗██╗   ██╗██████╗ "
echo "  ██╔════╝ ██║     ██╔════╝██╔══██╗████╗  ██║██║   ██║██╔══██╗"
echo "  ██║      ██║     █████╗  ███████║██╔██╗ ██║██║   ██║██████╔╝"
echo "  ██║      ██║     ██╔══╝  ██╔══██║██║╚██╗██║██║   ██║██╔═══╝ "
echo "  ╚██████╗ ███████╗███████╗██║  ██║██║ ╚████║╚██████╔╝██║     "
echo "   ╚═════╝ ╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝     "
echo -e "${NC}"
echo -e "${BOLD}  Cascata V1 — Script de Limpeza (AlmaLinux 9)${NC}"
echo ""

# =============================================================================
# DETECTAR DIRETÓRIO DO PROJETO
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "${SCRIPT_DIR}/SRS_CascataV1.md" ]]; then
    PROJECT_DIR="${SCRIPT_DIR}"
elif [[ -f "${SCRIPT_DIR}/../SRS_CascataV1.md" ]]; then
    PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
else
    PROJECT_DIR="${SCRIPT_DIR}"
fi

COMPOSE_DIR="${PROJECT_DIR}/infra/docker"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.shelter.yml"

# =============================================================================
# SELECIONAR MODO
# =============================================================================

MODE=""
case "${1:-}" in
    --soft)   MODE="1" ;;
    --medium) MODE="2" ;;
    --full)   MODE="3" ;;
    --nuke)   MODE="4" ;;
    *)
        echo -e "  ${BOLD}Selecione o nível de limpeza:${NC}"
        echo ""
        echo -e "    ${GREEN}1) Leve${NC}     — Para containers, remove containers parados"
        echo -e "    ${YELLOW}2) Média${NC}    — Leve + remove volumes ${RED}(apaga dados do banco!)${NC}"
        echo -e "    ${RED}3) Total${NC}    — Média + remove imagens Docker do Cascata"
        echo -e "    ${RED}${BOLD}4) Nuclear${NC}  — Total + remove Go, Rust, Node.js do Cascata"
        echo ""
        read -rp "  Opção [1-4]: " MODE
        ;;
esac

if [[ ! "${MODE}" =~ ^[1-4]$ ]]; then
    echo -e "\n${RED}Opção inválida. Use 1, 2, 3 ou 4.${NC}"
    exit 1
fi

LABELS=("" "LEVE" "MÉDIA" "TOTAL" "NUCLEAR")
echo ""
echo -e "  Modo selecionado: ${BOLD}${LABELS[${MODE}]}${NC}"

# Confirmação para modos destrutivos
if [[ "${MODE}" -ge 2 ]] && [[ "${1:-}" == "" ]]; then
    echo ""
    if [[ "${MODE}" -eq 2 ]]; then
        echo -e "  ${YELLOW}⚠  ATENÇÃO: Isso vai APAGAR todos os dados dos bancos!${NC}"
    elif [[ "${MODE}" -eq 3 ]]; then
        echo -e "  ${RED}⚠  ATENÇÃO: Isso vai APAGAR dados E imagens Docker!${NC}"
    elif [[ "${MODE}" -eq 4 ]]; then
        echo -e "  ${RED}${BOLD}⚠  ATENÇÃO: Isso vai APAGAR TUDO incluindo Go, Rust e Node.js!${NC}"
    fi
    echo ""
    read -rp "  Tem certeza? Digite 'sim' para confirmar: " CONFIRM
    if [[ "${CONFIRM}" != "sim" ]]; then
        echo -e "\n  Limpeza cancelada."
        exit 0
    fi
fi

# =============================================================================
# NÍVEL 1 — LEVE: Parar e remover containers
# =============================================================================

log_header "Nível 1 — Parando e removendo containers"

if [[ -f "${COMPOSE_FILE}" ]]; then
    cd "${COMPOSE_DIR}"
    RUNNING=$(docker compose -f docker-compose.shelter.yml ps --services --status running 2>/dev/null | wc -l)

    if [[ "${RUNNING}" -gt 0 ]]; then
        log_info "Parando ${RUNNING} serviços..."
        docker compose -f docker-compose.shelter.yml down 2>&1 || true
        log_ok "Containers parados e removidos"
    else
        log_ok "Nenhum container Cascata rodando no Compose"
    fi
else
    log_warn "docker-compose.shelter.yml não encontrado — pulando"
fi

# Limpar containers órfãos do Cascata
ORPHANS=$(docker ps -a --filter "name=cascata-" --format '{{.Names}}' 2>/dev/null | wc -l)
if [[ "${ORPHANS}" -gt 0 ]]; then
    log_info "Removendo ${ORPHANS} containers órfãos..."
    docker ps -a --filter "name=cascata-" --format '{{.Names}}' | xargs -r docker rm -f 2>/dev/null || true
    log_ok "Containers órfãos removidos"
fi

log_ok "Nível 1 concluído"

if [[ "${MODE}" -lt 2 ]]; then
    exit 0
fi

# =============================================================================
# NÍVEL 2 — MÉDIA: Remover volumes (dados dos bancos)
# =============================================================================

log_header "Nível 2 — Removendo volumes (dados persistentes)"

CASCATA_VOLUMES=(
    "cascata-yb-data"
    "cascata-ch-data"
    "cascata-rp-data"
    "cascata-df-data"
    "cascata-openbao-data"
    "cascata-vm-data"
)

for vol in "${CASCATA_VOLUMES[@]}"; do
    if docker volume inspect "${vol}" &>/dev/null; then
        docker volume rm "${vol}" 2>/dev/null || true
        log_ok "Volume removido: ${vol}"
    fi
done

# Remover volumes do docker compose que podem ter prefixo
for vol in $(docker volume ls --format '{{.Name}}' | grep -E "cascata|docker_cascata" 2>/dev/null); do
    docker volume rm "${vol}" 2>/dev/null || true
    log_ok "Volume limpo: ${vol}"
done

log_ok "Nível 2 concluído — dados apagados"

if [[ "${MODE}" -lt 3 ]]; then
    exit 0
fi

# =============================================================================
# NÍVEL 3 — TOTAL: Remover imagens Docker do Cascata
# =============================================================================

log_header "Nível 3 — Removendo imagens Docker do Cascata"

# Imagens customizadas do Cascata
for img in $(docker images --format '{{.Repository}}:{{.Tag}}' | grep "cascata/" 2>/dev/null); do
    docker rmi "${img}" 2>/dev/null || true
    log_ok "Imagem removida: ${img}"
done

BASE_IMAGES=(
    "yugabytedb/yugabyte"
    "docker.dragonflydb.io/dragonflydb/dragonfly"
    "docker.redpanda.com/redpandadata/redpanda"
    "clickhouse/clickhouse-server"
    "quay.io/openbao/openbao"
    "timberio/vector"
    "victoriametrics/victoria-metrics"
    "postgres"
)

echo ""
if [[ "${1:-}" == "" ]]; then
    read -rp "  Remover também as imagens base (YugabyteDB, Postgres, ClickHouse, etc.)? [s/N]: " RM_BASE
else
    RM_BASE="s"
fi

if [[ "${RM_BASE}" =~ ^[sS]$ ]]; then
    for base in "${BASE_IMAGES[@]}"; do
        for img in $(docker images --format '{{.Repository}}:{{.Tag}}' | grep "^${base}" 2>/dev/null); do
            docker rmi -f "${img}" 2>/dev/null || true
            log_ok "Imagem base removida: ${img}"
        done
    done
fi

docker image prune -f &>/dev/null || true
log_ok "Imagens dangling removidas e Nível 3 concluído."

if [[ "${MODE}" -lt 4 ]]; then
    exit 0
fi

# =============================================================================
# NÍVEL 4 — NUCLEAR: Remover ferramentas instaladas pelo Cascata
# =============================================================================

log_header "Nível 4 — Removendo ferramentas base e o diretório raíiz"

# Go
if [[ -d "/usr/local/go" ]]; then
    log_info "Removendo Go..."
    sudo rm -rf /usr/local/go
    sed -i '/# Go (adicionado pelo instalador Cascata)/d' "$HOME/.bashrc" 2>/dev/null || true
    sed -i '/export PATH=\$PATH:\/usr\/local\/go\/bin/d' "$HOME/.bashrc" 2>/dev/null || true
    sed -i '/export GOPATH=\$HOME\/go/d' "$HOME/.bashrc" 2>/dev/null || true
    sed -i '/export PATH=\$PATH:\$GOPATH\/bin/d' "$HOME/.bashrc" 2>/dev/null || true
    log_ok "Go removido"
fi

# Rust (rustup)
if [[ -d "$HOME/.rustup" ]]; then
    log_info "Removendo Rust via rustup..."
    rustup self uninstall -y 2>/dev/null || true
    rm -rf "$HOME/.cargo" "$HOME/.rustup" 2>/dev/null || true
    log_ok "Rust removido"
fi

# Node.js (nvm)
if [[ -d "$HOME/.nvm" ]]; then
    log_info "Removendo Node.js e nvm..."
    rm -rf "$HOME/.nvm"
    sed -i '/export NVM_DIR="$HOME\/.nvm"/d' "$HOME/.bashrc" 2>/dev/null || true
    sed -i '/\. "$NVM_DIR\/nvm\.sh"/d' "$HOME/.bashrc" 2>/dev/null || true
    sed -i '/\. "$NVM_DIR\/bash_completion"/d' "$HOME/.bashrc" 2>/dev/null || true
    log_ok "Node.js (NVM) removido"
fi

# Docker system prune massivo
log_info "Executando purgação completa do Docker (system prune --all --volumes)..."
docker system prune -af --volumes 2>/dev/null || true
log_ok "Docker varrido."

# Deletar diretório da V1 do Cascata
if [[ -d "${PROJECT_DIR}" ]]; then
    # Proteção monumental: Nunca, jamais deletar o /home ou o diretório raiz
    if [[ "${PROJECT_DIR}" != "/" ]] && [[ "${PROJECT_DIR}" != "$HOME" ]]; then
        log_info "Removendo diretório master CascataV1 (${PROJECT_DIR})..."
        cd "$HOME" || true
        rm -rf "${PROJECT_DIR}" 2>/dev/null || true
        log_ok "O código do CascataV1 foi deletado da máquina host."
    else
        log_warn "O projeto parece estar instalado na raiz do usuário. Deleção evitada por segurança."
    fi
fi

log_header "LIMPEZA NUCLEAR CONCLUÍDA"
echo -e "  ${GREEN}✔ O servior foi devolvido ao estado limpo.${NC}"
echo -e "  Aviso: O daemon do Docker ainda está instalado via DNF.${NC}"
echo ""
