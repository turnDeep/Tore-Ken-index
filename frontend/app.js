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

            const dynamicContainer = document.getElementById('dashboard-dynamic-content');
            dynamicContainer.innerHTML = ''; // Clear

            // 2. Render Short Term Charts
            for (const ticker of config.short_term) {
                await renderMarketAnalysisSection(ticker, dynamicContainer);
            }

            // 3. Render Long Term Charts
            for (const ticker of config.long_term) {
                renderLongTermSection(ticker, dynamicContainer);
            }

        } catch (error) {
            console.error("Failed to initialize dashboard:", error);
        }
    }

    async function renderMarketAnalysisSection(ticker, container) {
        // Create HTML Structure
        const section = document.createElement('div');
        section.className = 'market-section';
        section.innerHTML = `
            <h3>Market Analysis (${ticker})</h3>
            <div class="market-analysis-container">
                <div id="chart-wrapper-${ticker}" style="position: relative; width: 100%; overflow: hidden;">
                    <img id="chart-img-${ticker}" src="/api/stock-chart/${ticker}_market_chart.png" alt="${ticker} Chart" style="width: 100%; display: block;">
                    <div id="cursor-${ticker}" style="position: absolute; top: 0; bottom: 0; width: 2px; background-color: black; border-left: 1px dashed black; opacity: 0.3; pointer-events: none; display: none;"></div>
                </div>

                <div class="controls-container">
                    <button id="prev-${ticker}" class="control-btn">&lt;</button>
                    <div class="slider-container">
                            <input type="range" id="slider-${ticker}" min="0" max="0" value="0" style="width: 100%;">
                    </div>
                    <button id="next-${ticker}" class="control-btn">&gt;</button>
                </div>

                <div style="text-align: center; margin-top: 10px;">
                    <span id="date-${ticker}" class="date-display">--</span>
                    <span id="status-${ticker}" class="status-text status-neutral">--</span>
                </div>
            </div>
        `;
        container.appendChild(section);

        // Fetch Data
        try {
            const res = await fetchWithAuth(`/api/market-analysis?ticker=${ticker}`);
            if (!res.ok) throw new Error(`Failed to load data for ${ticker}`);
            const data = await res.json();

            if (data.history && data.history.length > 0) {
                // Initialize State
                dashboardState.charts[ticker] = {
                    history: data.history,
                    currentIndex: data.history.length - 1
                };

                // Setup Logic
                setupChartControls(ticker);
                updateChartDailyView(ticker, dashboardState.charts[ticker].currentIndex);

                // Update global last updated (using first valid one)
                const lastUpdatedEl = document.getElementById('last-updated');
                if (lastUpdatedEl && data.last_updated) {
                    lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`;
                }
            }
        } catch (e) {
            console.error(e);
            section.innerHTML += `<p style="color:red; text-align:center;">Failed to load data.</p>`;
        }
    }

    function setupChartControls(ticker) {
        const slider = document.getElementById(`slider-${ticker}`);
        const prevBtn = document.getElementById(`prev-${ticker}`);
        const nextBtn = document.getElementById(`next-${ticker}`);
        const state = dashboardState.charts[ticker];

        slider.min = 0;
        slider.max = state.history.length - 1;
        slider.value = state.currentIndex;

        slider.addEventListener('input', (e) => {
            const idx = parseInt(e.target.value);
            updateChartDailyView(ticker, idx);
        });

        prevBtn.addEventListener('click', () => {
            if (state.currentIndex > 0) {
                updateChartDailyView(ticker, state.currentIndex - 1);
                slider.value = state.currentIndex;
            }
        });

        nextBtn.addEventListener('click', () => {
            if (state.currentIndex < state.history.length - 1) {
                updateChartDailyView(ticker, state.currentIndex + 1);
                slider.value = state.currentIndex;
            }
        });
    }

    function updateChartDailyView(ticker, index) {
        const state = dashboardState.charts[ticker];
        state.currentIndex = index;
        const item = state.history[index];

        // Update Date
        document.getElementById(`date-${ticker}`).textContent = item.date;

        // Update Status
        const badge = document.getElementById(`status-${ticker}`);
        badge.textContent = item.status_text;
        badge.className = 'status-text';
        if (item.status_text.includes("Red to")) badge.classList.add('status-green');
        else if (item.status_text.includes("Green to")) badge.classList.add('status-red');
        else if (item.status_text.includes("Green")) badge.classList.add('status-green');
        else if (item.status_text.includes("Red")) badge.classList.add('status-red');
        else badge.classList.add('status-neutral');

        // Update Cursor
        const cursor = document.getElementById(`cursor-${ticker}`);
        const marginLeft = 0.05;
        const marginRight = 0.12;
        const plotWidthPct = 1.0 - marginLeft - marginRight;
        const count = state.history.length;
        const pct = (index + 0.5) / count;
        const leftPos = (marginLeft + (pct * plotWidthPct)) * 100;

        cursor.style.left = `${leftPos}%`;
        cursor.style.display = 'block';
    }

    function renderLongTermSection(ticker, container) {
        const section = document.createElement('div');
        section.style.cssText = 'margin-top: 20px; padding: 10px; background: #fff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.05);';

        const title = document.createElement('h4');
        title.textContent = `${ticker} Analysis (Long Term)`;
        section.appendChild(title);

        const imgWrapper = document.createElement('div');
        const img = document.createElement('img');
        img.alt = `${ticker} Strong Stock Chart`;
        img.style.cssText = 'width: 100%; display: block; border: 1px solid #eee;';

        // Cache busting
        const ts = new Date().getTime();
        img.src = `/api/stock-chart/${ticker}_strong_stock.png?t=${ts}`;

        img.onerror = () => {
            section.style.display = 'none';
            console.log(`Missing long term chart for ${ticker}`);
        };

        imgWrapper.appendChild(img);
        section.appendChild(imgWrapper);
        container.appendChild(section);
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
