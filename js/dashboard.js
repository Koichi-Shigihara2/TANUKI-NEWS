// 経済指標ダッシュボード用JavaScript (GitHub Pages / CSV読み込み最適化版)

let economicData = [];
let surpriseChart = null;
let correlationChart = null;

// --- ユーティリティ関数 ---

function parseJapaneseNumber(str) {
    if (!str || str === 'NaN' || str === '-' || str === '') return null;
    const cleaned = str.toString().replace(/,/g, '');
    const num = parseFloat(cleaned);
    return isNaN(num) ? null : num;
}

function formatNumber(num, decimals = 2) {
    if (num === null || num === undefined || isNaN(num)) return '-';
    return num.toLocaleString('ja-JP', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function getSurpriseClass(surprise) {
    const num = parseFloat(surprise);
    if (isNaN(num)) return 'neutral';
    return num > 0 ? 'text-green-600 font-bold' : num < 0 ? 'text-red-600 font-bold' : 'neutral';
}

function getSurpriseIcon(surprise) {
    const num = parseFloat(surprise);
    if (isNaN(num)) return '→';
    return num > 0 ? '↑' : num < 0 ? '↓' : '→';
}

// --- データ読み込み ---

async function loadEconomicData() {
    // ステップ1: APIを試みる（失敗しても続行）
    try {
        const response = await fetch('tables/economic_indicators?page=1&limit=100&sort=リリース日');
        if (response.ok) {
            const result = await response.json();
            economicData = result.data || [];
        }
        // response.ok が false でも throw しない → CSVフォールバックへ進む
    } catch (error) {
        console.log('APIが利用不可、CSVにフォールバック:', error.message);
    }

    // ステップ2: APIデータが空ならCSVを読み込む
    if (economicData.length === 0) {
        try {
            const csvResponse = await fetch('data/economic_history.csv');
            if (csvResponse.ok) {
                const csvText = await csvResponse.text();
                economicData = parseCSVData(csvText);
            } else {
                showError('CSVファイルが見つかりません');
                return;
            }
        } catch (csvError) {
            console.error('CSV読み込みエラー:', csvError);
            showError('データの読み込みに失敗しました');
            return;
        }
    }

    if (economicData.length === 0) {
        showError('データが見つかりません');
        return;
    }

    updateDashboard();
}

function parseCSVData(csvText) {
    const lines = csvText.trim().split('\n');
    if (lines.length < 2) return [];

    // ヘッダーの取得
    const headers = lines[0].split(',').map(h => h.trim());
    
    return lines.slice(1).map((line, index) => {
        // カンマ区切りのパース（簡易版）
        const values = line.split(',');
        const row = {};
        headers.forEach((header, i) => {
            row[header] = values[i] ? values[i].trim() : '';
        });
        row.id = `row_${index}`;
        return row;
    });
}

// --- 画面更新ロジック ---

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
    
    // 最新の有効な株価データを取得
    const validPrices = economicData.filter(d => parseJapaneseNumber(d['S&P500']) !== null);
    const latestPriceRow = validPrices.length > 0 ? validPrices[validPrices.length - 1] : null;
    const sp500Val = latestPriceRow ? parseJapaneseNumber(latestPriceRow['S&P500']) : null;

    economicData.forEach(row => {
        const surprise = parseFloat(row['Surprise(実際-期待)']);
        if (!isNaN(surprise)) {
            if (surprise > 0) positiveSurprises++;
            else if (surprise < 0) negativeSurprises++;
        }
    });

    document.getElementById('totalIndicators').textContent = totalIndicators;
    document.getElementById('positiveSurprises').textContent = positiveSurprises;
    document.getElementById('negativeSurprises').textContent = negativeSurprises;
    
    const sp500Element = document.getElementById('sp500Change');
    if (sp500Val) {
        sp500Element.textContent = formatNumber(sp500Val);
        sp500Element.className = `text-2xl font-bold ${sp500Val >= 5000 ? 'text-green-600' : 'text-red-600'}`;
    }
}

function updateCharts() {
    updateSurpriseChart();
    updateCorrelationChart();
}

function updateSurpriseChart() {
    const canvas = document.getElementById('surpriseChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    
    const sortedData = [...economicData]
        .filter(row => row['リリース日'] && !isNaN(parseFloat(row['Surprise(実際-期待)'])))
        .sort((a, b) => new Date(a['リリース日']) - new Date(b['リリース日']));

    const labels = sortedData.map(row => row['リリース日']);
    const surprises = sortedData.map(row => parseFloat(row['Surprise(実際-期待)']));

    if (surpriseChart) surpriseChart.destroy();

    surpriseChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'サプライズ (実際-期待)',
                data: surprises,
                backgroundColor: surprises.map(v => v >= 0 ? 'rgba(59, 130, 246, 0.6)' : 'rgba(239, 68, 68, 0.6)'),
                borderColor: surprises.map(v => v >= 0 ? '#3b82f6' : '#ef4444'),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true } }
        }
    });
}

function updateCorrelationChart() {
    const chartDom = document.getElementById('correlationChart');
    if (!chartDom) return;
    const myChart = echarts.init(chartDom);
    
    const plotData = economicData
        .filter(row => row['S&P500'] && row['Nasdaq'])
        .sort((a, b) => new Date(a['リリース日']) - new Date(b['リリース日']));

    const dates = plotData.map(row => row['リリース日']);
    const sp500Data = plotData.map(row => parseJapaneseNumber(row['S&P500']));
    const nasdaqData = plotData.map(row => parseJapaneseNumber(row['Nasdaq']));

    const option = {
        tooltip: { trigger: 'axis' },
        legend: { data: ['S&P500', 'Nasdaq'] },
        xAxis: { type: 'category', data: dates },
        yAxis: { type: 'value', scale: true },
        series: [
            { name: 'S&P500', type: 'line', data: sp500Data, smooth: true },
            { name: 'Nasdaq', type: 'line', data: nasdaqData, smooth: true }
        ]
    };
    myChart.setOption(option);
    window.addEventListener('resize', () => myChart.resize());
}

function updateTable() {
    const tbody = document.getElementById('tableBody');
    const indicatorFilter = document.getElementById('indicatorFilter').value;
    
    let filteredData = [...economicData];
    if (indicatorFilter) {
        filteredData = filteredData.filter(row => row['指標名'] === indicatorFilter);
    }
    
    filteredData.sort((a, b) => new Date(b['リリース日']) - new Date(a['リリース日']));

    if (filteredData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="px-6 py-4 text-center">データがありません</td></tr>';
        return;
    }

    tbody.innerHTML = filteredData.map(row => {
        const surprise = row['Surprise(実際-期待)'];
        return `
            <tr class="hover:bg-gray-50 border-b">
                <td class="px-6 py-4 text-sm font-medium text-gray-900">${row['指標名'] || '-'}</td>
                <td class="px-6 py-4 text-sm text-gray-500">${row['リリース日'] || '-'}</td>
                <td class="px-6 py-4 text-sm text-gray-900">${row['実際値'] || '-'}</td>
                <td class="px-6 py-4 text-sm text-gray-500">${row['期待値(Consensus)'] || '-'}</td>
                <td class="px-6 py-4 text-sm ${getSurpriseClass(surprise)}">${getSurpriseIcon(surprise)} ${surprise || '-'}</td>
                <td class="px-6 py-4 text-xs text-gray-600">${row['市場反応(自動生成)'] || '-'}</td>
                <td class="px-6 py-4 text-sm text-gray-900">${row['S&P500'] || '-'}</td>
            </tr>
        `;
    }).join('');
}

function updateFilters() {
    const filter = document.getElementById('indicatorFilter');
    if (!filter || filter.options.length > 1) return;
    
    const indicators = [...new Set(economicData.map(row => row['指標名']).filter(Boolean))];
    indicators.forEach(name => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        filter.appendChild(opt);
    });
}

function updateLastUpdate() {
    const el = document.getElementById('lastUpdate');
    if (el) el.textContent = new Date().toLocaleString('ja-JP');
}

function showError(msg) {
    const tbody = document.getElementById('tableBody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="px-6 py-4 text-center text-red-500">${msg}</td></tr>`;
}

// 起動
document.addEventListener('DOMContentLoaded', () => {
    loadEconomicData();
    // 1時間おきに自動更新
    setInterval(loadEconomicData, 60 * 60 * 1000);
});
