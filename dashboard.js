// 経済指標ダッシュボード用JavaScript

let economicData = [];
let surpriseChart = null;
let correlationChart = null;

// データフォーマット変換関数
function parseJapaneseNumber(str) {
    if (!str || str === 'NaN' || str === '-') return null;
    
    // 日本語の数値を数値に変換
    const cleaned = str.toString().replace(/,/g, '');
    const num = parseFloat(cleaned);
    return isNaN(num) ? null : num;
}

function formatNumber(num, decimals = 2) {
    if (num === null || num === undefined || isNaN(num)) return '-';
    return num.toFixed(decimals);
}

function formatCurrency(num) {
    if (num === null || num === undefined || isNaN(num)) return '-';
    return new Intl.NumberFormat('ja-JP', {
        style: 'currency',
        currency: 'JPY',
        minimumFractionDigits: 0
    }).format(num);
}

function getSurpriseClass(surprise) {
    const num = parseFloat(surprise);
    if (isNaN(num)) return 'neutral';
    return num > 0 ? 'positive' : num < 0 ? 'negative' : 'neutral';
}

function getSurpriseIcon(surprise) {
    const num = parseFloat(surprise);
    if (isNaN(num)) return '→';
    return num > 0 ? '↑' : num < 0 ? '↓' : '→';
}

// データ読み込み関数
async function loadEconomicData() {
    try {
        // テーブルAPIからデータを取得
        const response = await fetch('tables/economic_indicators?page=1&limit=100&sort=リリース日');
        if (!response.ok) throw new Error('データ取得失敗');
        
        const result = await response.json();
        economicData = result.data || [];
        
        // CSVファイルからのフォールバック
        if (economicData.length === 0) {
            const csvResponse = await fetch('data/05_economic_history.csv');
            if (csvResponse.ok) {
                const csvText = await csvResponse.text();
                economicData = parseCSVData(csvText);
            }
        }
        
        updateDashboard();
    } catch (error) {
        console.error('データ読み込みエラー:', error);
        showError('データの読み込みに失敗しました');
    }
}

function parseCSVData(csvText) {
    const lines = csvText.split('\n');
    const headers = lines[0].split(',');
    const data = [];
    
    for (let i = 1; i < lines.length; i++) {
        if (!lines[i].trim()) continue;
        
        const values = lines[i].split(',');
        const row = {};
        
        headers.forEach((header, index) => {
            row[header.trim()] = values[index]?.trim() || '';
        });
        
        // IDを生成
        row.id = `row_${i}`;
        row.created_at = new Date().getTime();
        row.updated_at = new Date().getTime();
        
        data.push(row);
    }
    
    return data;
}

// ダッシュボード更新関数
function updateDashboard() {
    updateSummaryCards();
    updateCharts();
    updateTable();
    updateFilters();
    updateLastUpdate();
}

function updateSummaryCards() {
    const totalIndicators = economicData.length;
    
    let positiveSurprises = 0;
    let negativeSurprises = 0;
    let sp500Change = null;
    
    economicData.forEach(row => {
        const surprise = parseFloat(row['Surprise(実際-期待)']);
        if (!isNaN(surprise)) {
            if (surprise > 0) positiveSurprises++;
            else if (surprise < 0) negativeSurprises++;
        }
        
        if (row['S&P500'] && sp500Change === null) {
            sp500Change = parseJapaneseNumber(row['S&P500']);
        }
    });
    
    document.getElementById('totalIndicators').textContent = totalIndicators;
    document.getElementById('positiveSurprises').textContent = positiveSurprises;
    document.getElementById('negativeSurprises').textContent = negativeSurprises;
    
    const sp500Element = document.getElementById('sp500Change');
    if (sp500Change !== null) {
        sp500Element.textContent = formatNumber(sp500Change);
        sp500Element.className = `text-2xl font-bold ${sp500Change >= 0 ? 'positive' : 'negative'}`;
    } else {
        sp500Element.textContent = '--';
        sp500Element.className = 'text-2xl font-bold neutral';
    }
}

function updateCharts() {
    updateSurpriseChart();
    updateCorrelationChart();
}

function updateSurpriseChart() {
    const ctx = document.getElementById('surpriseChart').getContext('2d');
    
    // サプライズデータを日付順にソート
    const sortedData = [...economicData]
        .filter(row => row['リリース日'] && row['Surprise(実際-期待)'])
        .sort((a, b) => new Date(a['リリース日']) - new Date(b['リリース日']));
    
    const labels = sortedData.map(row => row['リリース日']);
    const surprises = sortedData.map(row => parseFloat(row['Surprise(実際-期待)']) || 0);
    
    if (surpriseChart) {
        surpriseChart.destroy();
    }
    
    surpriseChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'サプライズ',
                data: surprises,
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

function updateCorrelationChart() {
    const chartDom = document.getElementById('correlationChart');
    const myChart = echarts.init(chartDom);
    
    // S&P500とNasdaqの相関データ
    const sp500Data = economicData.map(row => parseJapaneseNumber(row['S&P500'])).filter(val => val !== null);
    const nasdaqData = economicData.map(row => parseJapaneseNumber(row['Nasdaq'])).filter(val => val !== null);
    const dates = economicData.map(row => row['リリース日']).filter((_, index) => sp500Data[index] !== undefined);
    
    const option = {
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross'
            }
        },
        legend: {
            data: ['S&P500', 'Nasdaq']
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            boundaryGap: false,
            data: dates
        },
        yAxis: {
            type: 'value',
            axisLabel: {
                formatter: '${value}'
            }
        },
        series: [
            {
                name: 'S&P500',
                type: 'line',
                stack: 'Total',
                areaStyle: {},
                emphasis: {
                    focus: 'series'
                },
                data: sp500Data
            },
            {
                name: 'Nasdaq',
                type: 'line',
                stack: 'Total',
                areaStyle: {},
                emphasis: {
                    focus: 'series'
                },
                data: nasdaqData
            }
        ]
    };
    
    myChart.setOption(option);
    
    // レスポンシブ対応
    window.addEventListener('resize', function() {
        myChart.resize();
    });
}

function updateTable() {
    const tbody = document.getElementById('tableBody');
    const indicatorFilter = document.getElementById('indicatorFilter').value;
    const dateFilter = document.getElementById('dateFilter').value;
    
    let filteredData = economicData;
    
    if (indicatorFilter) {
        filteredData = filteredData.filter(row => row['指標名'] === indicatorFilter);
    }
    
    if (dateFilter) {
        filteredData = filteredData.filter(row => row['リリース日'] === dateFilter);
    }
    
    // 最新のデータを上に表示
    filteredData.sort((a, b) => new Date(b['リリース日']) - new Date(a['リリース日']));
    
    if (filteredData.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="px-6 py-4 text-center text-gray-500">
                    データが見つかりません
                </td>
            </tr>
        `;
        return;
    }
    
    tbody.innerHTML = filteredData.map(row => {
        const surprise = row['Surprise(実際-期待)'];
        const surpriseClass = getSurpriseClass(surprise);
        const surpriseIcon = getSurpriseIcon(surprise);
        
        return `
            <tr class="hover:bg-gray-50">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    ${row['指標名'] || '-'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    ${row['リリース日'] || '-'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    ${row['実際値'] || '-'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    ${row['期待値(Consensus)'] || '-'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium ${surpriseClass}">
                    ${surpriseIcon} ${row['Surprise(実際-期待)'] || '-'}
                </td>
                <td class="px-6 py-4 text-sm text-gray-900">
                    ${row['市場反応(自動生成)'] || '-'}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    ${row['S&P500'] || '-'}
                </td>
            </tr>
        `;
    }).join('');
}

function updateFilters() {
    const indicatorFilter = document.getElementById('indicatorFilter');
    const indicators = [...new Set(economicData.map(row => row['指標名']).filter(Boolean))];
    
    indicatorFilter.innerHTML = '<option value="">すべての指標</option>' +
        indicators.map(indicator => `<option value="${indicator}">${indicator}</option>`).join('');
}

function updateLastUpdate() {
    const now = new Date();
    document.getElementById('lastUpdate').textContent = now.toLocaleString('ja-JP');
}

function filterTable() {
    updateTable();
}

function refreshData() {
    const loadingElement = document.querySelector('#tableBody tr:first-child td');
    const originalContent = loadingElement.innerHTML;
    
    loadingElement.innerHTML = '<div class="loading"></div> 更新中...';
    
    setTimeout(() => {
        loadEconomicData();
    }, 1000);
}

function showError(message) {
    const tbody = document.getElementById('tableBody');
    tbody.innerHTML = `
        <tr>
            <td colspan="7" class="px-6 py-4 text-center text-red-600">
                <i class="fas fa-exclamation-triangle mr-2"></i>
                ${message}
            </td>
        </tr>
    `;
}

// データベーススキーマの初期化
async function initializeDatabase() {
    try {
        // 経済指標テーブルのスキーマ定義
        await fetch('tables/economic_indicators', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                name: 'economic_indicators',
                fields: [
                    { name: 'id', type: 'text', description: 'ユニークID' },
                    { name: '指標名', type: 'text', description: '経済指標名' },
                    { name: 'リリース日', type: 'datetime', description: '発表日' },
                    { name: '実際値', type: 'text', description: '実際の数値' },
                    { name: '期待値(Consensus)', type: 'text', description: '市場期待値' },
                    { name: '前回値', type: 'text', description: '前回の数値' },
                    { name: 'Surprise(実際-期待)', type: 'text', description: 'サプライズ' },
                    { name: 'YoY変化(%)', type: 'text', description: '前年比変化率' },
                    { name: 'S&P500', type: 'text', description: 'S&P500終値' },
                    { name: 'Nasdaq', type: 'text', description: 'Nasdaq終値' },
                    { name: '10Y-2Y(YieldCurve)', type: 'text', description: 'イールドカーブ' },
                    { name: '付随データ', type: 'rich_text', description: '追加データ' },
                    { name: '市場反応(自動生成)', type: 'text', description: '市場反応' },
                    { name: 'データソース', type: 'text', description: 'データ取得元' },
                    { name: '更新日時', type: 'datetime', description: '最終更新日時' }
                ]
            })
        });
    } catch (error) {
        console.log('データベース初期化スキップ:', error.message);
    }
}

// ページ読み込み時の処理
document.addEventListener('DOMContentLoaded', function() {
    initializeDatabase();
    loadEconomicData();
    
    // 定期更新（5分ごと）
    setInterval(loadEconomicData, 5 * 60 * 1000);
});