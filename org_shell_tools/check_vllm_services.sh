#!/usr/bin/env bash
# ============================================================
# check_vllm_services.sh
# 从 URL 列表批量测试 vLLM API 服务是否正常
# ============================================================



URLS_bak=(


"http://10.112.8.21:8600/v1/chat/completions"
"http://10.112.8.21:8601/v1/chat/completions"
"http://10.112.8.21:8602/v1/chat/completions"
"http://10.112.8.21:8603/v1/chat/completions"
"http://10.112.8.22:8600/v1/chat/completions"
"http://10.112.8.22:8601/v1/chat/completions"
"http://10.112.8.22:8602/v1/chat/completions"
"http://10.112.8.22:8603/v1/chat/completions"
"http://10.112.8.23:8600/v1/chat/completions"
"http://10.112.8.23:8601/v1/chat/completions"
"http://10.112.8.23:8602/v1/chat/completions"
"http://10.112.8.23:8603/v1/chat/completions"
"http://10.112.8.24:8600/v1/chat/completions"
"http://10.112.8.24:8601/v1/chat/completions"
"http://10.112.8.24:8602/v1/chat/completions"
"http://10.112.8.24:8603/v1/chat/completions"
"http://10.112.8.27:8600/v1/chat/completions"
"http://10.112.8.27:8601/v1/chat/completions"
"http://10.112.8.27:8602/v1/chat/completions"
"http://10.112.8.27:8603/v1/chat/completions"
"http://10.112.8.28:8600/v1/chat/completions"
"http://10.112.8.28:8601/v1/chat/completions"
"http://10.112.8.28:8602/v1/chat/completions"
"http://10.112.8.28:8603/v1/chat/completions"
"http://10.112.8.30:8600/v1/chat/completions"
"http://10.112.8.30:8601/v1/chat/completions"
"http://10.112.8.30:8602/v1/chat/completions"
"http://10.112.8.30:8603/v1/chat/completions"
"http://10.112.8.31:8600/v1/chat/completions"
"http://10.112.8.31:8601/v1/chat/completions"
"http://10.112.8.31:8602/v1/chat/completions"
"http://10.112.8.31:8603/v1/chat/completions"
"http://10.112.8.32:8600/v1/chat/completions"
"http://10.112.8.32:8601/v1/chat/completions"
"http://10.112.8.32:8602/v1/chat/completions"
"http://10.112.8.32:8603/v1/chat/completions"
"http://10.112.8.33:8600/v1/chat/completions"
"http://10.112.8.33:8601/v1/chat/completions"
"http://10.112.8.33:8602/v1/chat/completions"
"http://10.112.8.33:8603/v1/chat/completions"
"http://10.112.8.34:8600/v1/chat/completions"
"http://10.112.8.34:8601/v1/chat/completions"
"http://10.112.8.34:8602/v1/chat/completions"
"http://10.112.8.34:8603/v1/chat/completions"
"http://10.112.8.35:8600/v1/chat/completions"
"http://10.112.8.35:8601/v1/chat/completions"
"http://10.112.8.35:8602/v1/chat/completions"
"http://10.112.8.35:8603/v1/chat/completions"
"http://10.112.17.9:8600/v1/chat/completions"
"http://10.112.17.9:8601/v1/chat/completions"
"http://10.112.17.9:8602/v1/chat/completions"
"http://10.112.17.9:8603/v1/chat/completions"
"http://10.112.17.10:8600/v1/chat/completions"
"http://10.112.17.10:8601/v1/chat/completions"
"http://10.112.17.10:8602/v1/chat/completions"
"http://10.112.17.10:8603/v1/chat/completions"
"http://10.112.31.18:8600/v1/chat/completions"
"http://10.112.31.18:8601/v1/chat/completions"
"http://10.112.31.18:8602/v1/chat/completions"
"http://10.112.31.18:8603/v1/chat/completions"
"http://10.112.31.19:8600/v1/chat/completions"
"http://10.112.31.19:8601/v1/chat/completions"
"http://10.112.31.19:8602/v1/chat/completions"
"http://10.112.31.19:8603/v1/chat/completions"
"http://10.112.31.20:8600/v1/chat/completions"
"http://10.112.31.20:8601/v1/chat/completions"
"http://10.112.31.20:8602/v1/chat/completions"
"http://10.112.31.20:8603/v1/chat/completions"
"http://10.112.31.21:8600/v1/chat/completions"
"http://10.112.31.21:8601/v1/chat/completions"
"http://10.112.31.21:8602/v1/chat/completions"
"http://10.112.31.21:8603/v1/chat/completions"
"http://10.112.31.23:8600/v1/chat/completions"
"http://10.112.31.23:8601/v1/chat/completions"
"http://10.112.31.23:8602/v1/chat/completions"
"http://10.112.31.23:8603/v1/chat/completions"
"http://10.112.35.9:8600/v1/chat/completions"
"http://10.112.35.9:8601/v1/chat/completions"
"http://10.112.35.9:8602/v1/chat/completions"
"http://10.112.35.9:8603/v1/chat/completions"
"http://10.112.36.13:8600/v1/chat/completions"
"http://10.112.36.13:8601/v1/chat/completions"
"http://10.112.36.13:8602/v1/chat/completions"
"http://10.112.36.13:8603/v1/chat/completions"
"http://10.112.37.1:8600/v1/chat/completions"
"http://10.112.37.1:8601/v1/chat/completions"
"http://10.112.37.1:8602/v1/chat/completions"
"http://10.112.37.1:8603/v1/chat/completions"
"http://10.112.37.14:8600/v1/chat/completions"
"http://10.112.37.14:8601/v1/chat/completions"
"http://10.112.37.14:8602/v1/chat/completions"
"http://10.112.37.14:8603/v1/chat/completions"
"http://10.112.37.15:8600/v1/chat/completions"
"http://10.112.37.15:8601/v1/chat/completions"
"http://10.112.37.15:8602/v1/chat/completions"
"http://10.112.37.15:8603/v1/chat/completions"
"http://10.112.37.16:8600/v1/chat/completions"
"http://10.112.37.16:8601/v1/chat/completions"
"http://10.112.37.16:8602/v1/chat/completions"
"http://10.112.37.16:8603/v1/chat/completions"
"http://10.112.37.17:8600/v1/chat/completions"
"http://10.112.37.17:8601/v1/chat/completions"
"http://10.112.37.17:8602/v1/chat/completions"
"http://10.112.37.17:8603/v1/chat/completions"
"http://10.112.37.18:8600/v1/chat/completions"
"http://10.112.37.18:8601/v1/chat/completions"
"http://10.112.37.18:8602/v1/chat/completions"
"http://10.112.37.18:8603/v1/chat/completions"
"http://10.112.37.19:8600/v1/chat/completions"
"http://10.112.37.19:8601/v1/chat/completions"
"http://10.112.37.19:8602/v1/chat/completions"
"http://10.112.37.19:8603/v1/chat/completions"
"http://10.112.37.20:8600/v1/chat/completions"
"http://10.112.37.20:8601/v1/chat/completions"
"http://10.112.37.20:8602/v1/chat/completions"
"http://10.112.37.20:8603/v1/chat/completions"
"http://10.112.37.21:8600/v1/chat/completions"
"http://10.112.37.21:8601/v1/chat/completions"
"http://10.112.37.21:8602/v1/chat/completions"
"http://10.112.37.21:8603/v1/chat/completions"
"http://10.112.37.24:8600/v1/chat/completions"
"http://10.112.37.24:8601/v1/chat/completions"
"http://10.112.37.24:8602/v1/chat/completions"
"http://10.112.37.24:8603/v1/chat/completions"
"http://10.112.64.37:8600/v1/chat/completions"
"http://10.112.64.37:8601/v1/chat/completions"
"http://10.112.64.37:8602/v1/chat/completions"
"http://10.112.64.37:8603/v1/chat/completions"
"http://10.112.64.47:8600/v1/chat/completions"
"http://10.112.64.47:8601/v1/chat/completions"
"http://10.112.64.47:8602/v1/chat/completions"
"http://10.112.64.47:8603/v1/chat/completions"
"http://10.112.64.49:8600/v1/chat/completions"
"http://10.112.64.49:8601/v1/chat/completions"
"http://10.112.64.49:8602/v1/chat/completions"
"http://10.112.64.49:8603/v1/chat/completions"
"http://10.112.64.50:8600/v1/chat/completions"
"http://10.112.64.50:8601/v1/chat/completions"
"http://10.112.64.50:8602/v1/chat/completions"
"http://10.112.64.50:8603/v1/chat/completions"
"http://10.112.64.51:8600/v1/chat/completions"
"http://10.112.64.51:8601/v1/chat/completions"
"http://10.112.64.51:8602/v1/chat/completions"
"http://10.112.64.51:8603/v1/chat/completions"
"http://10.112.64.52:8600/v1/chat/completions"
"http://10.112.64.52:8601/v1/chat/completions"
"http://10.112.64.52:8602/v1/chat/completions"
"http://10.112.64.52:8603/v1/chat/completions"



)


URLS=(


"http://10.112.64.55:8000/v1/chat/completions"
"http://10.112.64.55:8001/v1/chat/completions"
"http://10.112.64.55:8002/v1/chat/completions"
"http://10.112.64.55:8003/v1/chat/completions"
"http://10.112.64.55:8004/v1/chat/completions"
"http://10.112.64.55:8005/v1/chat/completions"
"http://10.112.64.55:8006/v1/chat/completions"
"http://10.112.64.55:8007/v1/chat/completions"



)

URLS_tmp=(


"http://10.112.31.18:8600/v1/chat/completions"
"http://10.112.31.18:8601/v1/chat/completions"
"http://10.112.31.18:8602/v1/chat/completions"
"http://10.112.31.18:8603/v1/chat/completions"
"http://10.112.31.19:8600/v1/chat/completions"
"http://10.112.31.19:8601/v1/chat/completions"
"http://10.112.31.19:8602/v1/chat/completions"
"http://10.112.31.19:8603/v1/chat/completions"
"http://10.112.31.20:8600/v1/chat/completions"
"http://10.112.31.20:8601/v1/chat/completions"
"http://10.112.31.20:8602/v1/chat/completions"
"http://10.112.31.20:8603/v1/chat/completions"
"http://10.112.31.21:8600/v1/chat/completions"
"http://10.112.31.21:8601/v1/chat/completions"
"http://10.112.31.21:8602/v1/chat/completions"
"http://10.112.31.21:8603/v1/chat/completions"
"http://10.112.36.13:8600/v1/chat/completions"
"http://10.112.36.13:8601/v1/chat/completions"
"http://10.112.36.13:8602/v1/chat/completions"
"http://10.112.36.13:8603/v1/chat/completions"
"http://10.112.37.18:8600/v1/chat/completions"
"http://10.112.37.18:8601/v1/chat/completions"
"http://10.112.37.18:8602/v1/chat/completions"
"http://10.112.37.18:8603/v1/chat/completions"
"http://10.112.37.21:8600/v1/chat/completions"
"http://10.112.37.21:8601/v1/chat/completions"
"http://10.112.37.21:8602/v1/chat/completions"
"http://10.112.37.21:8603/v1/chat/completions"


)

TOKEN="sk-76e59c53d7234799ac6f"
MODEL="qwen2.5-0.5b"
# MODEL="Qwen3.6-35B-A3B-FP8"
# MODEL="qwen3-1.7b"
# MODEL="Qwen3.6-27B-NVFP4"
TIMEOUT=15

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

OK=0
FAIL=0
FAIL_LIST=()

printf "\n%-22s %-6s %-12s %s\n" "IP" "PORT" "STATUS" "DETAIL"
printf "%-22s %-6s %-12s %s\n" "----------------------" "------" "------------" "-------"

for url in "${URLS[@]}"; do
    # 从 URL 中解析 IP 和端口
    tmp="${url#http://}"          # 去掉 http://
    host_port="${tmp%%/*}"        # 取 IP:PORT 部分
    ip="${host_port%%:*}"
    port="${host_port##*:}"

    response=$(curl -s -o /tmp/_vllm_resp.json -w "%{http_code}" \
        --max-time "$TIMEOUT" \
        --request POST "$url" \
        --header 'Content-Type: application/json' \
        --header "Authorization: Bearer ${TOKEN}" \
        --header 'Accept: */*' \
        --header 'Connection: keep-alive' \
        --data "{
          \"model\": \"${MODEL}\",
          \"messages\": [{\"role\": \"user\", \"content\": \"hi\"}],
          \"max_tokens\": 5
        }" 2>/dev/null)

    http_code="$response"

    if [[ "$http_code" == "200" ]]; then
        content=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/_vllm_resp.json'))
    print(d['choices'][0]['message']['content'].strip().replace('\n',' ')[:40])
except:
    print('parse error')
" 2>/dev/null)
        printf "${GREEN}%-22s %-6s %-12s %s${NC}\n" "$ip" "$port" "OK[$http_code]" "↩ $content"
        (( OK++ ))
    elif [[ -z "$http_code" || "$http_code" == "000" ]]; then
        printf "${RED}%-22s %-6s %-12s %s${NC}\n" "$ip" "$port" "TIMEOUT" "无法连接或超时(>${TIMEOUT}s)"
        (( FAIL++ ))
        FAIL_LIST+=("${ip}:${port} - TIMEOUT")
    else
        errmsg=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/_vllm_resp.json'))
    print(str(d.get('message', d)).replace('\n',' ')[:50])
except:
    print(open('/tmp/_vllm_resp.json').read()[:50].replace('\n',' '))
" 2>/dev/null)
        printf "${YELLOW}%-22s %-6s %-12s %s${NC}\n" "$ip" "$port" "ERR[$http_code]" "$errmsg"
        (( FAIL++ ))
        FAIL_LIST+=("${ip}:${port} - HTTP $http_code")
    fi
done

# ---- 汇总 ----
total=$(( OK + FAIL ))
echo ""
echo "=============================="
printf "合计: %d 个接口\n" "$total"
printf "${GREEN}成功: %d${NC}\n" "$OK"
printf "${RED}失败: %d${NC}\n" "$FAIL"

if [[ ${#FAIL_LIST[@]} -gt 0 ]]; then
    echo ""
    echo "失败接口清单："
    for f in "${FAIL_LIST[@]}"; do
        echo "  ✗ $f"
    done
fi

rm -f /tmp/_vllm_resp.json
