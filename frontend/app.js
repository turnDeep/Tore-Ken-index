// ==========================================
// グローバルスコープ（ファイル先頭）
// ==========================================

// --- IndexedDB & Auth Config ---
const DB_NAME = 'ToreKenDB';
const DB_VERSION = 1;
const TOKEN_STORE_NAME = 'auth-tokens';

// --- Authentication Management (with IndexedDB support) ---
class AuthManager {
    static TOKEN_KEY = 'auth_token';
    static EXPIRY_KEY = 'auth_expiry';
    static PERMISSION_KEY = 'auth_permission';

    static async setTokenInDB(token) {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);
            request.onerror = () => reject("Error opening DB for token storage");
            request.onupgradeneeded = event => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(TOKEN_STORE_NAME)) {
                    db.createObjectStore(TOKEN_STORE_NAME, { keyPath: 'id' });
                }
            };
            request.onsuccess = event => {
                const db = event.target.result;
                const transaction = db.transaction([TOKEN_STORE_NAME], 'readwrite');
                const store = transaction.objectStore(TOKEN_STORE_NAME);
                if (token) {
                    store.put({ id: 'auth_token', value: token });
                } else {
                    store.delete('auth_token');
                }
                transaction.oncomplete = () => resolve();
                transaction.onerror = () => reject("Error storing token in DB");
            };
        });
    }

    static async setAuthData(token, expiresIn, permission) {
        localStorage.setItem(this.TOKEN_KEY, token);
        const expiryTime = Date.now() + (expiresIn * 1000);
        localStorage.setItem(this.EXPIRY_KEY, expiryTime.toString());
        localStorage.setItem(this.PERMISSION_KEY, permission);
        try {
            await this.setTokenInDB(token);
            console.log(`Auth token and permission (${permission}) stored. Expires at:`, new Date(expiryTime).toLocaleString());
        } catch (error) {
            console.error("Failed to store token in IndexedDB:", error);
        }
    }

    static getToken() {
        const token = localStorage.getItem(this.TOKEN_KEY);
        const expiry = localStorage.getItem(this.EXPIRY_KEY);
        if (!token || !expiry || Date.now() > parseInt(expiry)) {
            if (token) this.clearAuthData();
            return null;
        }
        return token;
    }

    static getPermission() {
        return localStorage.getItem(this.PERMISSION_KEY);
    }

    static async clearAuthData() {
        localStorage.removeItem(this.TOKEN_KEY);
        localStorage.removeItem(this.EXPIRY_KEY);
        localStorage.removeItem(this.PERMISSION_KEY);
        try {
            await this.setTokenInDB(null);
            console.log('Auth data cleared from localStorage and IndexedDB');
        } catch (error) {
            console.error("Failed to clear token from IndexedDB:", error);
        }
    }

    static isAuthenticated() {
        return this.getToken() !== null;
    }

    static getAuthHeaders() {
        const token = this.getToken();
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    }
}

// --- Helper for VAPID Key ---
function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/-/g, '+')
        .replace(/_/g, '/');

    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);

    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
}

// --- Authenticated Fetch Wrapper ---
async function fetchWithAuth(url, options = {}) {
    const authHeaders = AuthManager.getAuthHeaders();
    const response = await fetch(url, {
        ...options,
        headers: { ...options.headers, ...authHeaders }
    });

    if (response.status === 401) {
        console.log('Authentication failed (401), redirecting to auth screen');
        await AuthManager.clearAuthData();
        window.dispatchEvent(new CustomEvent('auth-required'));
        throw new Error('Authentication required');
    }
    return response;
}

// --- NotificationManager (from HanaView) ---
class NotificationManager {
    constructor() {
        this.isSupported = 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
        this.vapidPublicKey = null;
    }

    async init() {
        if (!this.isSupported) {
            console.log('Push notifications are not supported');
            return;
        }
        console.log('Initializing NotificationManager...');
        try {
            const response = await fetch('/api/vapid-public-key');
            const data = await response.json();
            this.vapidPublicKey = data.public_key;
            console.log('VAPID public key obtained');
        } catch (error) {
            console.error('Failed to get VAPID public key:', error);
            return;
        }
        const permission = await this.requestPermission();
        if (permission) {
            await this.subscribeUser();
        }
        navigator.serviceWorker.addEventListener('message', event => {
            if (event.data.type === 'data-updated' && event.data.data) {
                console.log('Data updated via push notification');
                // renderAllData not available here, but we can refresh
                // or show notification
                this.showInAppNotification('データが更新されました');
                location.reload();
            }
        });
    }

    async requestPermission() {
        const permission = await Notification.requestPermission();
        console.log('Notification permission:', permission);
        return permission === 'granted';
    }

    async subscribeUser() {
        try {
            const registration = await navigator.serviceWorker.ready;
            let subscription = await registration.pushManager.getSubscription();
            if (!subscription) {
                const convertedVapidKey = this.urlBase64ToUint8Array(this.vapidPublicKey);
                subscription = await registration.pushManager.subscribe({
                    userVisibleOnly: true,
                    applicationServerKey: convertedVapidKey
                });
            }
            await this.sendSubscriptionToServer(subscription);
            if ('sync' in registration) {
                await registration.sync.register('data-sync');
            }
        } catch (error) {
            console.error('Failed to subscribe user:', error);
        }
    }

    async sendSubscriptionToServer(subscription) {
        try {
            if (typeof AuthManager === 'undefined') {
                console.error('❌ AuthManager is not defined yet');
                throw new Error('認証マネージャーが読み込まれていません。');
            }

            if (!AuthManager.isAuthenticated()) {
                console.warn('Cannot register push subscription: not authenticated');
                return;
            }

            console.log('📤 Sending push subscription to server...');

            const response = await fetchWithAuth('/api/subscribe', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(subscription)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Server returned ${response.status}: ${errorText}`);
            }

            const result = await response.json();
            console.log('✅ Push subscription registered:', result);
            this.showInAppNotification(`通知が有効になりました (権限: ${result.permission})`);
        } catch (error) {
            console.error('❌ Error sending subscription to server:', error);
            let errorMessage = error.message || '不明なエラー';
            alert(`⚠️ Push通知の登録に失敗しました:\n${errorMessage}`);
        }
    }

    urlBase64ToUint8Array(base64String) {
        const padding = '='.repeat((4 - base64String.length % 4) % 4);
        const base64 = (base64String + padding)
            .replace(/\-/g, '+')
            .replace(/_/g, '/');
        const rawData = window.atob(base64);
        const outputArray = new Uint8Array(rawData.length);
        for (let i = 0; i < rawData.length; ++i) {
            outputArray[i] = rawData.charCodeAt(i);
        }
        return outputArray;
    }

    showInAppNotification(message) {
        const toast = document.createElement('div');
        toast.className = 'toast-notification';
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #006B6B;
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 10000;
            animation: slideIn 0.3s ease-out;
        `;
        document.body.appendChild(toast);
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => {
                if (toast.parentNode) document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }
}

// ==========================================
// DOMContentLoaded以降
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
    console.log("Tore-ken App Initializing...");

    // --- DOM Element References ---
    const authContainer = document.getElementById('auth-container');
    const dashboardContainer = document.querySelector('.container');
    const pinInputsContainer = document.getElementById('pin-inputs');
    const pinInputs = pinInputsContainer ? Array.from(pinInputsContainer.querySelectorAll('input')) : [];
    const authErrorMessage = document.getElementById('auth-error-message');
    const authSubmitButton = document.getElementById('auth-submit-button');
    const authLoadingSpinner = document.getElementById('auth-loading');

    // --- State ---
    let failedAttempts = 0;
    const MAX_ATTEMPTS = 5;
    let globalNotificationManager = null;
    let marketHistory = [];
    let currentDateIndex = -1;

    // ✅ 認証エラーイベントのリスナー追加
    window.addEventListener('auth-required', () => {
        showAuthScreen();
    });

    // --- Main App Logic ---
    async function initializeApp() {
        // ✅ 古い認証データのクリーンアップ
        if (localStorage.getItem('auth_token') && !localStorage.getItem('auth_permission')) {
            console.log('🧹 Cleaning old authentication data...');
            await AuthManager.clearAuthData();
            if ('serviceWorker' in navigator) {
                const registrations = await navigator.serviceWorker.getRegistrations();
                for (let registration of registrations) {
                    await registration.unregister();
                }
            }
            alert('⚠️ 認証システムが更新されました。再度ログインしてください。');
            location.reload();
            return;
        }

        try {
            if (AuthManager.isAuthenticated()) {
                await showDashboard();
            } else {
                showAuthScreen();
            }
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error('Error during authentication check:', error);
                if (authErrorMessage) authErrorMessage.textContent = 'サーバーとの通信に失敗しました。';
            }
            showAuthScreen();
        }
    }

    async function showDashboard() {
        if (authContainer) authContainer.style.display = 'none';
        if (dashboardContainer) dashboardContainer.style.display = 'block';

        if (typeof AuthManager === 'undefined' || typeof fetchWithAuth === 'undefined') {
            console.error('❌ Required dependencies not loaded. Skipping notification setup.');
            alert('⚠️ アプリの初期化に問題があります。ページを再読み込みしてください。');
            return;
        }

        if (!globalNotificationManager) {
            globalNotificationManager = new NotificationManager();
            try {
                // 少し待機してからNotificationManagerを初期化（iPhone PWA対策）
                await new Promise(resolve => setTimeout(resolve, 100));
                await globalNotificationManager.init();
                console.log('✅ Notifications initialized');
            } catch (error) {
                console.error('❌ Notification initialization failed:', error);
            }
        }

        if (!dashboardContainer.dataset.initialized) {
            console.log("Tore-ken Dashboard Initialized");
            fetchDataAndRender();
            dashboardContainer.dataset.initialized = 'true';
        }
    }

    function showAuthScreen() {
        if (authContainer) authContainer.style.display = 'flex';
        if (dashboardContainer) dashboardContainer.style.display = 'none';
        setupAuthForm();
    }

    function setupAuthForm() {
        if (!pinInputsContainer) return;
        pinInputs.forEach(input => { input.value = ''; input.disabled = false; });
        if(authSubmitButton) authSubmitButton.disabled = false;
        if(authErrorMessage) authErrorMessage.textContent = '';
        failedAttempts = 0;
        pinInputs[0]?.focus();

        pinInputs.forEach((input, index) => {
            input.addEventListener('input', () => {
                if (input.value.length === 1 && index < pinInputs.length - 1) {
                    pinInputs[index + 1].focus();
                }
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                    pinInputs[index - 1].focus();
                }
            });
            input.addEventListener('paste', (e) => {
                e.preventDefault();
                const pasteData = e.clipboardData.getData('text').trim();
                if (/^\d{6}$/.test(pasteData)) {
                    pasteData.split('').forEach((char, i) => { if (pinInputs[i]) pinInputs[i].value = char; });
                    handleAuthSubmit();
                }
            });
        });

        if (authSubmitButton) {
            const newButton = authSubmitButton.cloneNode(true);
            authSubmitButton.parentNode.replaceChild(newButton, authSubmitButton);
            newButton.addEventListener('click', handleAuthSubmit);
        }
    }

    async function handleAuthSubmit() {
        const pin = pinInputs.map(input => input.value).join('');
        if (pin.length !== 6) {
            if (authErrorMessage) authErrorMessage.textContent = '6桁のコードを入力してください。';
            return;
        }
        setLoading(true);
        try {
            const response = await fetch('/api/auth/verify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pin: pin })
            });
            const data = await response.json();
            if (response.ok && data.success) {
                await AuthManager.setAuthData(data.token, data.expires_in, data.permission);
                console.log('✅ Authentication complete, token saved');
                await showDashboard();
            } else {
                failedAttempts++;
                pinInputs.forEach(input => input.value = '');
                pinInputs[0].focus();
                if (failedAttempts >= MAX_ATTEMPTS) {
                    if (authErrorMessage) authErrorMessage.textContent = '認証に失敗しました。';
                    pinInputs.forEach(input => input.disabled = true);
                    document.getElementById('auth-submit-button').disabled = true;
                } else {
                    if (authErrorMessage) authErrorMessage.textContent = '正しい認証コードを入力してください。';
                }
            }
        } catch (error) {
            console.error('Error during PIN verification:', error);
            if (authErrorMessage) authErrorMessage.textContent = '認証中にエラーが発生しました。';
        } finally {
            setLoading(false);
        }
    }

    function setLoading(isLoading) {
        if (authLoadingSpinner) authLoadingSpinner.style.display = isLoading ? 'block' : 'none';
        const submitBtn = document.getElementById('auth-submit-button');
        if (submitBtn) submitBtn.style.display = isLoading ? 'none' : 'block';
    }

    // --- Dashboard Functions (Refactored) ---

    // State management for multiple charts
    const dashboardState = {
        charts: {} // { ticker: { history: [], currentIndex: 0 } }
    };

    async function fetchDataAndRender() {
        try {
            // 1. Fetch Config
            const configRes = await fetchWithAuth('/api/config/tickers');
            if (!configRes.ok) throw new Error("Failed to load config");
            const config = await configRes.json();

            // 2. Fetch Unified Data
            // We'll create a new endpoint /api/market-analysis-unified or update existing
            // For now, let's assume we can fetch the unified JSON directly via a new endpoint
            const dataRes = await fetchWithAuth('/api/market-analysis-unified');
            let unifiedData = {};
            if (dataRes.ok) {
                unifiedData = await dataRes.json();
            } else {
                console.warn("Unified data endpoint failed, falling back to legacy...");
            }

            const dynamicContainer = document.getElementById('dashboard-dynamic-content');
            dynamicContainer.innerHTML = ''; // Clear

            // 3. Render Charts grouped by Ticker
            const allTickers = Array.from(new Set([...config.short_term, ...config.long_term]));

            for (const ticker of allTickers) {
                const tickerData = unifiedData[ticker] || {};

                // Create Main Ticker Section
                const tickerSection = document.createElement('div');
                tickerSection.className = 'market-section';
                tickerSection.innerHTML = `<h3>${ticker} Analysis</h3>`;
                dynamicContainer.appendChild(tickerSection);

                // Render Short Term Chart if exists in config
                if (config.short_term.includes(ticker)) {
                    renderShortTermSubsection(ticker, tickerData.short_term, tickerSection, tickerData);
                }

                // Render Long Term Chart if exists in config
                if (config.long_term.includes(ticker)) {
                    renderLongTermSubsection(ticker, tickerData.long_term, tickerSection, tickerData);
                }
            }

            // Update Global Last Updated based on first available timestamp from unified data
            // Or just use the current time if successful
            const lastUpdatedEl = document.getElementById('last-updated');
            if (lastUpdatedEl) {
                // Find the latest update time across all data
                let latestTime = null;
                Object.values(unifiedData).forEach(t => {
                    if (t.short_term?.last_updated) {
                        const time = new Date(t.short_term.last_updated);
                        if (!latestTime || time > latestTime) latestTime = time;
                    }
                     if (t.long_term?.last_updated) {
                        const time = new Date(t.long_term.last_updated);
                        if (!latestTime || time > latestTime) latestTime = time;
                    }
                });

                if (latestTime) {
                    lastUpdatedEl.textContent = `Last updated: ${latestTime.toLocaleString('ja-JP')}`;
                }
            }

        } catch (error) {
            console.error("Failed to initialize dashboard:", error);
        }
    }

    function renderShortTermSubsection(ticker, shortTermData, container, allTickerData) {
        // Create Subsection Container
        const wrapper = document.createElement('div');
        wrapper.style.marginBottom = '20px'; // Spacing below this section

        // Sub-subtitle
        const subtitle = document.createElement('h4');
        subtitle.textContent = "Short Term";
        subtitle.style.cssText = "margin-bottom: 10px; font-weight: bold; color: #444;";
        wrapper.appendChild(subtitle);

        // Content
        wrapper.innerHTML += `
            <div class="market-analysis-container">
                <div id="chart-wrapper-${ticker}" style="position: relative; width: 100%; overflow: hidden;">
                    <img id="chart-img-${ticker}" src="/api/stock-chart/${ticker}_market_chart.png" alt="${ticker} Chart" style="width: 100%; display: block;">
                </div>

                <div style="text-align: center; margin-top: 10px;">
                    <span id="date-${ticker}" class="date-display">--</span>
                    <span id="status-${ticker}" class="status-text status-neutral">--</span>
                </div>
            </div>
        `;
        container.appendChild(wrapper);

        // Use passed data if available, otherwise try fetch (legacy fallback)
        if (shortTermData && shortTermData.history && shortTermData.history.length > 0) {
             const latestItem = shortTermData.history[shortTermData.history.length - 1];
             updateChartInfo(ticker, latestItem);
        } else {
            // Fallback fetch
            fetchWithAuth(`/api/market-analysis?ticker=${ticker}`)
                .then(res => res.json())
                .then(data => {
                    if (data.history && data.history.length > 0) {
                        const latestItem = data.history[data.history.length - 1];
                        updateChartInfo(ticker, latestItem);
                    }
                })
                .catch(e => {
                    console.error(`Failed to load legacy data for ${ticker}`, e);
                    wrapper.innerHTML += `<p style="color:red; text-align:center;">Data unavailable.</p>`;
                });
        }

        // Render Gemini Analysis for SPY here (below Short Term chart)
        if (ticker === 'SPY' && allTickerData && allTickerData.gemini_analysis) {
            renderGeminiAnalysis(wrapper, allTickerData.gemini_analysis);
        }
    }

    function updateChartInfo(ticker, item) {
        // Update Date
        const dateEl = document.getElementById(`date-${ticker}`);
        if (dateEl) dateEl.textContent = item.date;

        // Update Status
        const badge = document.getElementById(`status-${ticker}`);
        if (badge) {
            badge.textContent = item.status_text;
            badge.className = 'status-text';
            if (item.status_text.includes("Red to")) badge.classList.add('status-green');
            else if (item.status_text.includes("Green to")) badge.classList.add('status-red');
            else if (item.status_text.includes("Green")) badge.classList.add('status-green');
            else if (item.status_text.includes("Red")) badge.classList.add('status-red');
            else badge.classList.add('status-neutral');
        }
    }

    function renderLongTermSubsection(ticker, longTermData, container, allTickerData) {
        const wrapper = document.createElement('div');
        wrapper.style.cssText = 'margin-top: 20px; padding-top: 10px; border-top: 1px dashed #eee;';

        const subtitle = document.createElement('h4');
        subtitle.textContent = "Long Term";
        subtitle.style.cssText = "margin-bottom: 10px; font-weight: bold; color: #444;";
        wrapper.appendChild(subtitle);

        // Display metadata if available
        if (longTermData) {
            const infoDiv = document.createElement('div');
            infoDiv.style.cssText = 'font-size: 0.85em; color: #666; margin-bottom: 5px; text-align: right;';
            const dateStr = longTermData.data_date || (longTermData.last_updated ? new Date(longTermData.last_updated).toLocaleDateString() : 'Unknown');
            infoDiv.textContent = `Data Date: ${dateStr}`;
            wrapper.appendChild(infoDiv);
        }

        const imgWrapper = document.createElement('div');
        const img = document.createElement('img');
        img.alt = `${ticker} Strong Stock Chart`;
        img.style.cssText = 'width: 100%; display: block; border: 1px solid #eee;';

        // Cache busting
        const ts = new Date().getTime();
        // Use URL from data or default
        const srcUrl = (longTermData && longTermData.image_url) ? longTermData.image_url : `/api/stock-chart/${ticker}_strong_stock.png`;
        img.src = `${srcUrl}?t=${ts}`;

        img.onerror = () => {
            wrapper.style.display = 'none';
            console.log(`Missing long term chart for ${ticker}`);
        };

        imgWrapper.appendChild(img);
        wrapper.appendChild(imgWrapper);
        container.appendChild(wrapper);

        // Render Gemini Analysis for QQQ, SOXX, GLD here (below Long Term chart)
        // Check if ticker is NOT SPY (since SPY is handled in short term)
        if (ticker !== 'SPY' && allTickerData && allTickerData.gemini_analysis) {
            renderGeminiAnalysis(container, allTickerData.gemini_analysis);
        }
    }

    function renderGeminiAnalysis(container, analysisData) {
        if (!analysisData || !analysisData.content) return;

        const analysisDiv = document.createElement('div');
        analysisDiv.className = 'gemini-analysis';
        analysisDiv.style.cssText = `
            margin-top: 15px;
            padding: 15px;
            background-color: #f8f9fa;
            border-left: 4px solid #006B6B;
            border-radius: 4px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        `;

        analysisDiv.innerHTML = `
            <div class="gemini-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; padding-bottom: 8px; border-bottom: 1px solid #e9ecef;">
                <span class="ai-icon" style="font-weight: bold; color: #006B6B;">🤖 AI解説 (Gemini)</span>
                <span class="update-time" style="font-size: 0.8em; color: #6c757d;">${new Date(analysisData.updated_at).toLocaleString()}</span>
            </div>
        `;

        const contentDiv = document.createElement('div');
        contentDiv.className = 'gemini-content';
        contentDiv.style.cssText = "font-size: 0.95em; line-height: 1.6; color: #333; white-space: pre-wrap;";
        contentDiv.textContent = analysisData.content;

        analysisDiv.appendChild(contentDiv);
        container.appendChild(analysisDiv);
    }

    // --- Auto Reload Function ---
    function setupAutoReload() {
        // 5-minute Force Reload (PWA)
        setInterval(() => {
            console.log("5-minute force reload triggered");
            location.reload();
        }, 300000); // 300,000 ms = 5 minutes

        const LAST_RELOAD_KEY = 'lastAutoReloadDate';
        setInterval(() => {
            const now = new Date();
            const day = now.getDay();
            const hours = now.getHours();
            const minutes = now.getMinutes();
            const isWeekday = day >= 1 && day <= 5;
            const isReloadTime = hours === 6 && minutes === 30;
            if (isWeekday && isReloadTime) {
                const today = now.toISOString().split('T')[0];
                const lastReloadDate = localStorage.getItem(LAST_RELOAD_KEY);
                if (lastReloadDate !== today) {
                    console.log('Auto-reloading page at 6:30 on a weekday...');
                    localStorage.setItem(LAST_RELOAD_KEY, today);
                    location.reload();
                }
            }
        }, 60000);
    }

    // --- App Initialization ---
    initializeApp();
    setupAutoReload();
});
