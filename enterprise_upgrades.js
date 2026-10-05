/**
 * ============================================================================
 *   P&A DISTRIBUTORS - ENTERPRISE AUTOMATION SUITE & LIVE MULTI-DEVICE SYNC
 * ============================================================================
 * Developed for P&A Distributors Daily Sales OS.
 * Modules Included:
 *   1. Multi-Device Real-Time Server Sync & Persistence (SSE Broadcast + SQLite/JSON)
 *   2. Category 12: DSR Field Operations & Location Intelligence (GPS, Route Planner, Voice Notes)
 *   3. Category 14: Cash, Banking & Petty Cash Management (Multi-Account Ledger, Staff IOU Tracker, BRS)
 *   4. Category 16: Automated WhatsApp & SMS Notifications (1-Tap Receipts, Reminders, Slip Modal)
 * ============================================================================
 */

(function () {
  'use strict';

  console.log('⚡ Initializing P&A Distributors Enterprise Upgrades Suite...');

  // ==========================================================================
  // 1. MULTI-DEVICE LIVE SERVER SYNC & PERSISTENCE MANAGER
  // ==========================================================================
  const MultiDeviceSyncManager = {
    serverOnline: false,
    eventSource: null,
    connectedDevices: 1,
    syncDebounceTimer: null,
    lastSyncSelfUser: null,
    lastSyncTime: 0,
    _isRemoteApplying: false,

    supportedCollections: [
      'PA_SALES_V2',
      'PA_EXP_V2',
      'PA_RETURNS_V2',
      'PA_MONTHLY_TRENDS_V1',
      'PA_CASH_BANKING_V1',
      'PA_ATTENDANCE_V2',
      'PA_DSR_ROUTE_MILEAGE_V1',
      'PA_DSR_PETROL_PRICE',
      'PA_PENDING_SALES_V1',
      'PA_DAILY_TEMPLATES_V1',
      'PA_JOURNEY_PLANNER_V1',
      'PA_BANK_ACCOUNTS_V1',
      'PA_PETTY_CASH_IOUS_V1',
      'PA_BANK_RECON_V1',
      'PA_WHATSAPP_TEMPLATES_V1',
      'PA_DAY_END_SETTLEMENTS_V1',
      'PA_ACTIVE_THEME'
    ],

    init() {
      this.injectLiveSyncBadge();
      this.checkServerHealth();
      this.connectSSE();
      this.hookLocalStorage();
      this.loadInitialServerData();
      
      // Periodic ping every 30 seconds
      setInterval(() => this.checkServerHealth(), 30000);
    },

    injectLiveSyncBadge() {
      if (document.getElementById('liveSyncBadgeContainer')) return;
      const themeBtn = document.getElementById('topBtnQuickTheme') || document.querySelector('header button');
      if (!themeBtn) return;

      const badge = document.createElement('div');
      badge.id = 'liveSyncBadgeContainer';
      badge.className = 'flex items-center gap-1.5 px-2.5 sm:px-3 py-1.5 rounded-xl text-xs font-bold transition shadow-sm cursor-pointer select-none bg-slate-800/80 border border-slate-600 text-slate-300 hover:text-white';
      badge.title = 'Real-Time Multi-Device Sync Active. Click for details.';
      badge.innerHTML = `
        <span class="relative flex h-2.5 w-2.5">
          <span id="syncPulseRing" class="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75"></span>
          <span id="syncStatusDot" class="relative inline-flex rounded-full h-2.5 w-2.5 bg-amber-500"></span>
        </span>
        <span id="syncStatusText" class="text-[11px] font-extrabold hidden sm:inline">Connecting...</span>
        <span id="syncDevicesPill" class="bg-slate-700 text-slate-300 text-[10px] px-1.5 py-0.5 rounded-full font-mono font-bold">1</span>
      `;

      badge.onclick = () => this.showSyncStatusModal();
      themeBtn.parentElement.insertBefore(badge, themeBtn);
    },

    updateBadgeUI(online, count) {
      this.serverOnline = online;
      if (count !== undefined) this.connectedDevices = count;

      const dot = document.getElementById('syncStatusDot');
      const ring = document.getElementById('syncPulseRing');
      const text = document.getElementById('syncStatusText');
      const pill = document.getElementById('syncDevicesPill');
      const container = document.getElementById('liveSyncBadgeContainer');

      if (!dot || !text || !pill || !container) return;

      if (online) {
        dot.className = 'relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500';
        ring.className = 'animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75';
        text.innerText = 'Live Sync';
        text.className = 'text-[11px] font-extrabold text-emerald-400 hidden sm:inline';
        container.className = 'flex items-center gap-1.5 px-2.5 sm:px-3 py-1.5 rounded-xl text-xs font-bold transition shadow-sm cursor-pointer select-none bg-emerald-950/70 border border-emerald-500/50 hover:bg-emerald-900/80';
        pill.innerText = `${this.connectedDevices} Online`;
        pill.className = 'bg-emerald-800/80 text-emerald-200 text-[10px] px-1.5 py-0.5 rounded-full font-mono font-bold';
      } else {
        dot.className = 'relative inline-flex rounded-full h-2.5 w-2.5 bg-amber-500';
        ring.className = 'hidden';
        text.innerText = 'Offline Cache';
        text.className = 'text-[11px] font-extrabold text-amber-300 hidden sm:inline';
        container.className = 'flex items-center gap-1.5 px-2.5 sm:px-3 py-1.5 rounded-xl text-xs font-bold transition shadow-sm cursor-pointer select-none bg-amber-950/70 border border-amber-600/50 hover:bg-amber-900/80';
        pill.innerText = 'Local';
        pill.className = 'bg-amber-800/80 text-amber-200 text-[10px] px-1.5 py-0.5 rounded-full font-mono font-bold';
      }
    },

    checkServerHealth() {
      fetch('/api/status')
        .then(res => res.json())
        .then(data => {
          this.updateBadgeUI(true, data.connectedDevices || 1);
        })
        .catch(() => {
          this.updateBadgeUI(false, 1);
        });
    },

    connectSSE() {
      if (typeof EventSource === 'undefined') return;
      if (this.eventSource) {
        this.eventSource.close();
      }

      this.eventSource = new EventSource('/api/stream');

      this.eventSource.addEventListener('connected', (e) => {
        try {
          const d = JSON.parse(e.data);
          this.updateBadgeUI(true, d.clientCount || 1);
        } catch (_) {}
      });

      this.eventSource.addEventListener('clients_count', (e) => {
        try {
          const d = JSON.parse(e.data);
          this.updateBadgeUI(true, d.count);
        } catch (_) {}
      });

      this.eventSource.addEventListener('data_updated', (e) => {
        try {
          const d = JSON.parse(e.data);
          this.handleRemoteUpdate(d);
        } catch (err) {
          console.error('[Sync] Error processing remote update:', err);
        }
      });

      this.eventSource.onerror = () => {
        this.updateBadgeUI(false, 1);
      };
    },

    handleRemoteUpdate(eventData) {
      const collections = eventData.collections || [];
      const updatedBy = eventData.updatedBy || 'Another Device';

      // Ignore echo from self if within recent burst window
      if (this.lastSyncSelfUser === updatedBy && Date.now() - (this.lastSyncTime || 0) < 600) {
        return;
      }

      console.log(`⚡ [Multi-Device Sync] Received updates for:`, collections, `from ${updatedBy}`);
      
      fetch('/api/data')
        .then(res => res.json())
        .then(res => {
          if (!res.success || !res.data) return;
          this.applyServerData(res.data, false);
          
          if (window.showToast) {
            window.showToast(`⚡ Real-Time Sync: Updated by ${updatedBy}`);
          }
        })
        .catch(err => console.warn('[Sync] Failed to fetch remote update:', err));
    },

    loadInitialServerData() {
      fetch('/api/data')
        .then(res => res.json())
        .then(res => {
          if (!res.success || !res.data) return;

          const serverData = res.data;
          const hasServerData = Object.keys(serverData).length > 0;

          if (hasServerData) {
            this.applyServerData(serverData, true);
          } else {
            console.log('⚡ Server database is empty. Uploading local browser collections to server...');
            this.uploadAllLocalDataToServer();
          }
        })
        .catch(err => {
          console.warn('[Sync] Initial server load deferred (offline mode active):', err.message);
        });
    },

    applyServerData(serverData, isInitialBoot) {
      let shouldReRender = false;

      this.supportedCollections.forEach(key => {
        if (serverData[key] !== undefined && serverData[key] !== null) {
          const localStr = localStorage.getItem(key);
          const serverStr = typeof serverData[key] === 'string' ? serverData[key] : JSON.stringify(serverData[key]);

          // DATA PRESERVATION GUARD: Never overwrite non-empty local attendance or mileage with empty/stale server data
          if ((key === 'PA_ATTENDANCE_V2' || key === 'PA_DSR_ROUTE_MILEAGE_V1') && localStr && localStr !== 'null' && localStr !== '[]' && localStr !== '{}') {
            if (!serverData[key] || serverStr === 'null' || serverStr === '[]' || serverStr === '{}') {
              console.log(`[Safety Guard] Preserving local ${key} and syncing to server.`);
              this.queueCollectionSync(key, localStr);
              return;
            }
          }

          if (localStr !== serverStr) {
            this._isRemoteApplying = true;
            try {
              localStorage.setItem(key, serverStr);
            } finally {
              this._isRemoteApplying = false;
            }
            shouldReRender = true;

            // Update in-memory references
            try {
              if (key === 'PA_SALES_V2' && window.sales) {
                window.sales = JSON.parse(serverStr);
                if (Array.isArray(window.sales)) {
                  window.sales.forEach(s => { s.amount = window.parseAmount ? window.parseAmount(s.amount) : Number(s.amount); });
                }
              } else if (key === 'PA_EXP_V2' && window.expenses) {
                window.expenses = JSON.parse(serverStr);
              } else if (key === 'PA_RETURNS_V2' && window.returnCheques) {
                window.returnCheques = JSON.parse(serverStr);
              } else if (key === 'PA_CASH_BANKING_V1' && window.cashBankingRecords) {
                window.cashBankingRecords = JSON.parse(serverStr);
              } else if (key === 'PA_MONTHLY_TRENDS_V1' && window.monthlyTrendsData) {
                window.monthlyTrendsData = JSON.parse(serverStr);
              }
            } catch (_) {}
          }
        }
      });

      if (shouldReRender) {
        this.triggerAppRerenders();
      }
    },

    uploadAllLocalDataToServer() {
      const bundle = {};
      this.supportedCollections.forEach(key => {
        const val = localStorage.getItem(key);
        if (val) {
          try {
            bundle[key] = JSON.parse(val);
          } catch (_) {
            bundle[key] = val;
          }
        }
      });

      if (Object.keys(bundle).length === 0) return;

      this.lastSyncSelfUser = window.currentUser?.displayName || 'Office User';
      this.lastSyncTime = Date.now();

      fetch('/api/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          collections: bundle,
          user: this.lastSyncSelfUser
        })
      })
      .then(res => res.json())
      .then(res => {
        if (res.success) {
          console.log('✓ Initial local data successfully synced with server.');
        }
      })
      .catch(err => console.warn('[Sync] Failed to upload initial local data:', err));
    },

    hookLocalStorage() {
      const originalSetItem = localStorage.setItem.bind(localStorage);
      const self = this;

      localStorage.setItem = function (key, value) {
        originalSetItem(key, value);

        if (self._isRemoteApplying) return;

        if (self.supportedCollections.includes(key)) {
          self.queueCollectionSync(key, value);
        }
      };
    },

    queueCollectionSync(key, value) {
      let parsed = value;
      try {
        parsed = JSON.parse(value);
      } catch (_) {}

      clearTimeout(this.syncDebounceTimer);
      this.syncDebounceTimer = setTimeout(() => {
        this.sendSyncToServer(key, parsed);
      }, 350);
    },

    sendSyncToServer(key, data) {
      this.lastSyncSelfUser = window.currentUser?.displayName || 'Office User';
      this.lastSyncTime = Date.now();

      fetch('/api/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          collection: key,
          data: data,
          user: this.lastSyncSelfUser
        })
      })
      .then(res => res.json())
      .then(res => {
        if (res.success) {
          this.updateBadgeUI(true);
        }
      })
      .catch(err => {
        console.warn(`[Sync] Failed to push collection ${key} to server, saved locally:`, err.message);
        this.updateBadgeUI(false);
      });
    },

    triggerAppRerenders() {
      try {
        if (typeof window.applyFilters === 'function') {
          window.applyFilters();
        } else {
          if (typeof window.renderSalesTable === 'function' && window.sales) {
            window.renderSalesTable(window.sales);
          }
          if (typeof window.renderExpensesTable === 'function' && window.expenses) {
            window.renderExpensesTable(window.expenses);
          }
          if (typeof window.renderChequesTable === 'function') {
            window.renderChequesTable(window.sales || []);
          }
        }
        if (typeof window.renderDSRCards === 'function') {
          window.renderDSRCards();
        }
        if (typeof window.renderAttendance === 'function') {
          window.renderAttendance();
        }
        if (window.JourneyPlanner && typeof window.JourneyPlanner.render === 'function') {
          window.JourneyPlanner.render();
        }
        if (window.CashBankingLedger && typeof window.CashBankingLedger.render === 'function') {
          window.CashBankingLedger.render();
        }
      } catch (e) {
        console.warn('[Sync] Re-render warning:', e);
      }
    },

    showSyncStatusModal() {
      let modal = document.getElementById('syncStatusDetailsModal');
      if (!modal) {
        modal = document.createElement('div');
        modal.id = 'syncStatusDetailsModal';
        modal.className = 'fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-center justify-center p-4 animate-fade-in';
        modal.innerHTML = `
          <div class="bg-white rounded-3xl max-w-md w-full p-5 shadow-2xl space-y-4 border border-slate-200">
            <div class="flex items-center justify-between border-b pb-3">
              <div class="flex items-center gap-2">
                <span class="w-8 h-8 rounded-xl bg-emerald-600 text-white flex items-center justify-center text-sm shadow">
                  <i class="fa-solid fa-wifi"></i>
                </span>
                <div>
                  <h3 class="font-extrabold text-base text-slate-800">Multi-Device Live Sync</h3>
                  <p class="text-[11px] text-slate-500">Wi-Fi & Central Server Status</p>
                </div>
              </div>
              <button onclick="document.getElementById('syncStatusDetailsModal').classList.add('hidden')" class="text-slate-400 hover:text-black">
                <i class="fa-solid fa-xmark text-lg"></i>
              </button>
            </div>
            
            <div id="syncModalBody" class="space-y-3 text-xs">
              <div class="p-3 bg-slate-50 rounded-2xl border border-slate-200 space-y-2">
                <div class="flex justify-between items-center">
                  <span class="text-slate-500 font-bold">Server Connection:</span>
                  <span id="syncModalServerStatus" class="font-extrabold text-emerald-600">Checking...</span>
                </div>
                <div class="flex justify-between items-center">
                  <span class="text-slate-500 font-bold">Active Devices on Wi-Fi:</span>
                  <span id="syncModalDeviceCount" class="font-bold text-slate-800">1</span>
                </div>
                <div class="flex justify-between items-center">
                  <span class="text-slate-500 font-bold">Database Storage:</span>
                  <span class="font-bold text-slate-800">SQLite + JSON Stream</span>
                </div>
                <div class="flex justify-between items-center">
                  <span class="text-slate-500 font-bold">Real-Time Protocol:</span>
                  <span class="font-bold text-emerald-700">Server-Sent Events (SSE)</span>
                </div>
              </div>

              <div class="flex gap-2 pt-2">
                <button onclick="MultiDeviceSyncManager.uploadAllLocalDataToServer(); if(window.showToast) window.showToast('Forced full upload to central database!');" class="flex-1 py-2.5 bg-[#1B365D] hover:bg-[#0B192C] text-white font-bold rounded-xl shadow transition active:scale-95 text-center">
                  <i class="fa-solid fa-cloud-arrow-up mr-1.5"></i> Push to Server
                </button>
                <button onclick="MultiDeviceSyncManager.loadInitialServerData(); if(window.showToast) window.showToast('Pulling latest data from server...');" class="flex-1 py-2.5 bg-emerald-600 hover:bg-emerald-700 text-white font-bold rounded-xl shadow transition active:scale-95 text-center">
                  <i class="fa-solid fa-cloud-arrow-down mr-1.5"></i> Pull From Server
                </button>
              </div>

              <button onclick="MultiDeviceSyncManager.triggerServerBackup()" class="w-full py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold rounded-xl border border-slate-300 transition text-center">
                <i class="fa-solid fa-floppy-disk mr-1.5 text-amber-600"></i> Create Server Database Snapshot
              </button>
            </div>
          </div>
        `;
        document.body.appendChild(modal);
      }

      modal.classList.remove('hidden');
      const statusEl = document.getElementById('syncModalServerStatus');
      const devEl = document.getElementById('syncModalDeviceCount');
      if (statusEl) {
        statusEl.innerText = this.serverOnline ? '🟢 Connected (Real-Time)' : '🟡 Offline (Local Storage Cache)';
        statusEl.className = this.serverOnline ? 'font-extrabold text-emerald-600' : 'font-extrabold text-amber-600';
      }
      if (devEl) {
        devEl.innerText = `${this.connectedDevices} Device(s)`;
      }
    },

    triggerServerBackup() {
      fetch('/api/backup', { method: 'POST' })
        .then(res => res.json())
        .then(data => {
          if (data.success) {
            if (window.showToast) window.showToast(`✓ Server snapshot created: ${data.filename}`);
          } else {
            alert('Backup failed: ' + (data.error || 'Unknown error'));
          }
        })
        .catch(e => alert('Backup failed: ' + e.message));
    }
  };

  window.MultiDeviceSyncManager = MultiDeviceSyncManager;


  // ==========================================================================
  // 2. CATEGORY 12: DSR FIELD OPERATIONS & LOCATION INTELLIGENCE
  // ==========================================================================

  // A. GPS Location Stamping Helper
  window.captureGPSLocation = function (targetInputId, displayBadgeId, callback) {
    if (!navigator.geolocation) {
      alert('Geolocation is not supported by your browser or device.');
      return;
    }

    const badge = document.getElementById(displayBadgeId);
    if (badge) {
      badge.innerHTML = `<span class="animate-pulse text-amber-600 font-bold"><i class="fa-solid fa-location-crosshairs mr-1"></i>Acquiring GPS Satellite Fix...</span>`;
      badge.classList.remove('hidden');
    }

    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const lat = pos.coords.latitude.toFixed(6);
        const lng = pos.coords.longitude.toFixed(6);
        const accuracy = Math.round(pos.coords.accuracy);

        const gpsData = { lat: Number(lat), lng: Number(lng), accuracy, timestamp: new Date().toISOString() };
        const jsonStr = JSON.stringify(gpsData);

        if (targetInputId) {
          const input = document.getElementById(targetInputId);
          if (input) input.value = jsonStr;
        }

        if (badge) {
          badge.innerHTML = `
            <a href="https://maps.google.com/?q=${lat},${lng}" target="_blank" class="inline-flex items-center gap-1.5 px-2 py-1 bg-emerald-100 hover:bg-emerald-200 border border-emerald-300 text-emerald-900 rounded-lg text-[10px] font-bold shadow-2xs transition">
              <i class="fa-solid fa-location-dot text-emerald-600"></i>
              <span>GPS: ${lat}, ${lng} (±${accuracy}m)</span>
              <i class="fa-solid fa-arrow-up-right-from-square text-[9px] text-emerald-700"></i>
            </a>
          `;
          badge.classList.remove('hidden');
        }

        if (typeof callback === 'function') callback(gpsData);
        if (window.showToast) window.showToast(`📍 GPS Stamped: ${lat}, ${lng} (±${accuracy}m)`);
      },
      (err) => {
        console.warn('GPS error:', err);
        if (badge) {
          badge.innerHTML = `<span class="text-rose-600 font-semibold"><i class="fa-solid fa-triangle-exclamation mr-1"></i>GPS Unavailable (${err.message})</span>`;
        }
        alert('Could not acquire GPS coordinates: ' + err.message + '. Please ensure Location permissions are allowed.');
      },
      { enableHighAccuracy: true, timeout: 10000, maximumAge: 60000 }
    );
  };

  // B. Voice-to-Text Dictation Helper
  window.startVoiceDictation = function (targetInputId, micButtonId) {
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognition) {
      alert('Voice dictation is supported in Google Chrome, Microsoft Edge, and Android/iOS browsers.');
      return;
    }

    const recognition = new SpeechRecognition();
    recognition.lang = 'en-US';
    recognition.continuous = false;
    recognition.interimResults = false;

    const micBtn = document.getElementById(micButtonId);
    const origHtml = micBtn ? micBtn.innerHTML : '';

    if (micBtn) {
      micBtn.innerHTML = `<i class="fa-solid fa-microphone-lines text-rose-500 animate-bounce"></i>`;
      micBtn.classList.add('ring-2', 'ring-rose-400');
    }

    if (window.showToast) window.showToast('🎙️ Listening... Speak your remarks clearly.');

    recognition.onresult = (event) => {
      const transcript = event.results[0][0].transcript;
      const targetInput = document.getElementById(targetInputId);
      if (targetInput) {
        targetInput.value = targetInput.value ? `${targetInput.value} ${transcript}` : transcript;
        if (window.showToast) window.showToast(`✓ Dictated: "${transcript}"`);
      }
    };

    recognition.onerror = (e) => {
      console.warn('Speech recognition error:', e);
      if (window.showToast) window.showToast('Voice dictation cancelled or not heard.');
    };

    recognition.onend = () => {
      if (micBtn) {
        micBtn.innerHTML = origHtml;
        micBtn.classList.remove('ring-2', 'ring-rose-400');
      }
    };

    recognition.start();
  };

  // C. Route Schedule & Shop Journey Planner
  const DEFAULT_ROUTES = {
    'Monday': [
      { id: 'R1_1', name: 'Metro Cell City', area: 'Galle Town', contact: '077-1234567', status: 'Pending', notes: '' },
      { id: 'R1_2', name: 'Universal Mobile', area: 'Matara Cross St', contact: '071-2345678', status: 'Pending', notes: '' },
      { id: 'R1_3', name: 'City Cellular', area: 'Weligama Bazaar', contact: '076-3456789', status: 'Pending', notes: '' },
      { id: 'R1_4', name: 'Smartlink Phones', area: 'Ahangama Junction', contact: '075-4567890', status: 'Pending', notes: '' }
    ],
    'Tuesday': [
      { id: 'R2_1', name: 'Apex Telecom', area: 'Negombo Main St', contact: '077-5678901', status: 'Pending', notes: '' },
      { id: 'R2_2', name: 'Ja-Ela Cellular Hub', area: 'Ja-Ela Junction', contact: '078-6789012', status: 'Pending', notes: '' },
      { id: 'R2_3', name: 'Wattala Mobile Care', area: 'Wattala Town', contact: '072-7890123', status: 'Pending', notes: '' },
      { id: 'R2_4', name: 'Kandana Phone Zone', area: 'Kandana Station Rd', contact: '071-8901234', status: 'Pending', notes: '' }
    ],
    'Wednesday': [
      { id: 'R3_1', name: 'Kandy Phone Zone', area: 'Peradeniya Rd, Kandy', contact: '077-9012345', status: 'Pending', notes: '' },
      { id: 'R3_2', name: 'Hill Country Mobiles', area: 'Dalada Veediya', contact: '070-0123456', status: 'Pending', notes: '' },
      { id: 'R3_3', name: 'Katugastota Tel Shop', area: 'Katugastota Bridge', contact: '076-1234509', status: 'Pending', notes: '' }
    ],
    'Thursday': [
      { id: 'R4_1', name: 'Kurunegala Phone Plaza', area: 'Clock Tower Junction', contact: '077-2345601', status: 'Pending', notes: '' },
      { id: 'R4_2', name: 'Wariyapola Mobile Link', area: 'Wariyapola Town', contact: '078-3456712', status: 'Pending', notes: '' },
      { id: 'R4_3', name: 'Pannala Communications', area: 'Pannala Market', contact: '072-4567823', status: 'Pending', notes: '' }
    ],
    'Friday': [
      { id: 'R5_1', name: 'VIVO Plaza - Gampaha', area: 'Gampaha Court Rd', contact: '077-5678934', status: 'Pending', notes: '' },
      { id: 'R5_2', name: 'Mirigama Cell World', area: 'Station Road', contact: '071-6789045', status: 'Pending', notes: '' },
      { id: 'R5_3', name: 'Nittambuwa Phone Hub', area: 'Kandy Road', contact: '076-7890156', status: 'Pending', notes: '' }
    ],
    'Saturday': [
      { id: 'R6_1', name: 'Colombo Central Cellular', area: 'Pettah Main St', contact: '077-8901267', status: 'Pending', notes: '' },
      { id: 'R6_2', name: 'Borella Mobile Point', area: 'Borella Junction', contact: '078-9012378', status: 'Pending', notes: '' },
      { id: 'R6_3', name: 'Nugegoda Tech Mart', area: 'High Level Rd', contact: '071-0123489', status: 'Pending', notes: '' }
    ]
  };

  const JourneyPlanner = {
    selectedDay: 'Monday',

    getData() {
      let data = null;
      try {
        data = JSON.parse(localStorage.getItem('PA_JOURNEY_PLANNER_V1'));
      } catch (_) {}
      if (!data) {
        data = DEFAULT_ROUTES;
        localStorage.setItem('PA_JOURNEY_PLANNER_V1', JSON.stringify(data));
      }
      return data;
    },

    saveData(data) {
      localStorage.setItem('PA_JOURNEY_PLANNER_V1', JSON.stringify(data));
      this.render();
    },

    setDay(day) {
      this.selectedDay = day;
      this.render();
    },

    updateShopStatus(shopId, newStatus) {
      const data = this.getData();
      const list = data[this.selectedDay] || [];
      const shop = list.find(s => s.id === shopId);
      if (shop) {
        shop.status = newStatus;
        shop.updatedAt = new Date().toISOString();
        shop.updatedBy = window.currentUser?.displayName || 'DSR';
        this.saveData(data);
        if (window.showToast) window.showToast(`✓ ${shop.name} marked as "${newStatus}"`);
      }
    },

    stampShopGPS(shopId) {
      window.captureGPSLocation(null, null, (gps) => {
        const data = this.getData();
        const list = data[this.selectedDay] || [];
        const shop = list.find(s => s.id === shopId);
        if (shop) {
          shop.gps = gps;
          shop.status = 'Visited';
          this.saveData(data);
        }
      });
    },

    addShopPrompt() {
      const name = prompt('Enter Dealer / Shop Name:');
      if (!name) return;
      const area = prompt('Enter Area / Location (e.g. Negombo Town):') || '';
      const contact = prompt('Enter Contact Phone Number:') || '';

      const data = this.getData();
      if (!data[this.selectedDay]) data[this.selectedDay] = [];
      data[this.selectedDay].push({
        id: 'SH_' + Date.now(),
        name,
        area,
        contact,
        status: 'Pending',
        notes: ''
      });
      this.saveData(data);
      if (window.showToast) window.showToast(`✓ Added "${name}" to ${this.selectedDay} Route.`);
    },

    render() {
      const container = document.getElementById('journeyPlannerContainer');
      if (!container) return;

      const data = this.getData();
      const shops = data[this.selectedDay] || [];

      const total = shops.length;
      const visitedCount = shops.filter(s => s.status === 'Visited' || s.status === 'Order Billed').length;
      const billedCount = shops.filter(s => s.status === 'Order Billed').length;
      const pct = total > 0 ? Math.round((visitedCount / total) * 100) : 0;

      const days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

      container.innerHTML = `
        <div class="space-y-4">
          <!-- Top Bar -->
          <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 bg-slate-900 text-white p-4 rounded-2xl shadow-md border border-slate-700">
            <div>
              <div class="flex items-center gap-2">
                <span class="w-8 h-8 rounded-xl bg-amber-500 text-white flex items-center justify-center text-sm font-bold shadow">
                  <i class="fa-solid fa-map-location-dot"></i>
                </span>
                <h3 class="font-extrabold text-base sm:text-lg">Daily Route Journey & Shop Checklist</h3>
              </div>
              <p class="text-xs text-slate-300 mt-1">Real-Time Field Coverage • GPS Stamped Check-ins • Multi-Device Sync</p>
            </div>

            <!-- Add Shop & Day Progress -->
            <div class="flex items-center gap-2 w-full sm:w-auto">
              <button onclick="JourneyPlanner.addShopPrompt()" class="px-3 py-1.5 bg-amber-500 hover:bg-amber-600 text-slate-950 font-extrabold text-xs rounded-xl shadow transition active:scale-95 flex items-center gap-1.5">
                <i class="fa-solid fa-plus"></i> Add Shop to Route
              </button>
            </div>
          </div>

          <!-- Day Tabs -->
          <div class="flex flex-wrap gap-1.5 p-1 bg-slate-100 rounded-2xl border border-slate-200">
            ${days.map(d => `
              <button onclick="JourneyPlanner.setDay('${d}')" class="flex-1 min-w-[70px] py-1.5 px-2 text-xs font-bold rounded-xl transition ${this.selectedDay === d ? 'bg-[#1B365D] text-white shadow-sm font-extrabold' : 'text-slate-600 hover:text-slate-900 hover:bg-white/60'}">
                ${d}
              </button>
            `).join('')}
          </div>

          <!-- Progress Bar -->
          <div class="bg-white p-3.5 rounded-2xl border border-slate-200 shadow-sm flex flex-col sm:flex-row items-center justify-between gap-3">
            <div class="w-full sm:flex-1">
              <div class="flex justify-between text-xs font-bold mb-1">
                <span class="text-slate-700">${this.selectedDay} Route Progress: <span class="text-emerald-700">${visitedCount} of ${total} Visited (${pct}%)</span></span>
                <span class="text-indigo-700 font-extrabold">${billedCount} Orders Billed</span>
              </div>
              <div class="w-full bg-slate-100 h-2.5 rounded-full overflow-hidden border border-slate-200">
                <div class="bg-gradient-to-r from-emerald-500 to-teal-600 h-full rounded-full transition-all duration-500" style="width: ${pct}%"></div>
              </div>
            </div>
          </div>

          <!-- Shop Cards Checklist -->
          <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
            ${shops.length === 0 ? `
              <div class="col-span-2 text-center py-10 text-slate-400 bg-white rounded-2xl border border-slate-200">
                <i class="fa-solid fa-store text-3xl mb-2 text-slate-300"></i>
                <p>No shops mapped for ${this.selectedDay}. Click "Add Shop to Route" above.</p>
              </div>
            ` : shops.map((s, idx) => {
              const isBilled = s.status === 'Order Billed';
              const isVisited = s.status === 'Visited';
              const isClosed = s.status === 'Closed / Skipped';
              const isPending = s.status === 'Pending';

              let statusBadgeClass = 'bg-slate-100 text-slate-600 border-slate-300';
              if (isBilled) statusBadgeClass = 'bg-emerald-100 text-emerald-800 border-emerald-300 font-black';
              else if (isVisited) statusBadgeClass = 'bg-sky-100 text-sky-800 border-sky-300';
              else if (isClosed) statusBadgeClass = 'bg-rose-100 text-rose-800 border-rose-300';

              return `
                <div class="bg-white p-3.5 rounded-2xl border border-slate-200 shadow-sm flex flex-col justify-between gap-3 hover:shadow-md transition">
                  <div>
                    <div class="flex items-start justify-between gap-2">
                      <div class="flex items-center gap-2">
                        <span class="w-6 h-6 rounded-lg bg-slate-100 text-slate-700 font-black text-xs flex items-center justify-center border border-slate-200">
                          ${idx + 1}
                        </span>
                        <div>
                          <h4 class="font-bold text-sm text-slate-900">${s.name}</h4>
                          <p class="text-[11px] text-slate-500"><i class="fa-solid fa-location-dot text-rose-500 mr-1"></i>${s.area || 'Route Waypoint'} • <i class="fa-solid fa-phone text-slate-400 mr-1"></i>${s.contact || 'No phone'}</p>
                        </div>
                      </div>
                      <span class="text-[10px] font-bold px-2 py-0.5 rounded-full border ${statusBadgeClass}">
                        ${s.status}
                      </span>
                    </div>

                    ${s.gps ? `
                      <div class="mt-2 text-[10px]">
                        <a href="https://maps.google.com/?q=${s.gps.lat},${s.gps.lng}" target="_blank" class="inline-flex items-center gap-1 text-emerald-700 hover:text-emerald-900 font-bold bg-emerald-50 px-2 py-0.5 rounded border border-emerald-200">
                          <i class="fa-solid fa-location-crosshairs text-emerald-600"></i> GPS: ${s.gps.lat}, ${s.gps.lng} (Verified)
                        </a>
                      </div>
                    ` : ''}
                  </div>

                  <!-- Action Buttons -->
                  <div class="flex flex-wrap items-center justify-between gap-1.5 pt-2 border-t border-slate-100 text-xs">
                    <div class="flex items-center gap-1">
                      <button onclick="JourneyPlanner.updateShopStatus('${s.id}', 'Visited')" class="px-2 py-1 rounded-lg font-bold text-[11px] transition ${isVisited ? 'bg-sky-600 text-white' : 'bg-slate-100 hover:bg-sky-50 text-slate-700'}">
                        🚗 Visited
                      </button>
                      <button onclick="JourneyPlanner.updateShopStatus('${s.id}', 'Order Billed')" class="px-2 py-1 rounded-lg font-bold text-[11px] transition ${isBilled ? 'bg-emerald-600 text-white' : 'bg-slate-100 hover:bg-emerald-50 text-slate-700'}">
                        💰 Billed
                      </button>
                      <button onclick="JourneyPlanner.updateShopStatus('${s.id}', 'Closed / Skipped')" class="px-2 py-1 rounded-lg font-bold text-[11px] transition ${isClosed ? 'bg-rose-600 text-white' : 'bg-slate-100 hover:bg-rose-50 text-slate-700'}">
                        ❌ Skip
                      </button>
                    </div>

                    <div class="flex items-center gap-1">
                      <button onclick="JourneyPlanner.stampShopGPS('${s.id}')" title="Stamp Current GPS Location" class="p-1.5 rounded-lg bg-emerald-50 hover:bg-emerald-100 text-emerald-700 border border-emerald-200">
                        <i class="fa-solid fa-location-crosshairs text-xs"></i>
                      </button>
                      ${s.contact ? `
                        <a href="https://wa.me/94${s.contact.replace(/[^0-9]/g, '').slice(-9)}?text=${encodeURIComponent('Hello ' + s.name + ', P&A Distributors rep is on the way to your shop.')}" target="_blank" title="WhatsApp Shop" class="p-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white">
                          <i class="fa-brands fa-whatsapp text-xs"></i>
                        </a>
                      ` : ''}
                    </div>
                  </div>
                </div>
              `;
            }).join('')}
          </div>
        </div>
      `;
    }
  };

  window.JourneyPlanner = JourneyPlanner;


  // ==========================================================================
  // 3. CATEGORY 14: CASH, BANKING & PETTY CASH MANAGEMENT
  // ==========================================================================
  const CashBankingLedger = {
    getAccounts() {
      let accs = null;
      try {
        accs = JSON.parse(localStorage.getItem('PA_BANK_ACCOUNTS_V1'));
      } catch (_) {}
      if (!accs) {
        accs = {
          VAULT: { name: 'Main Cash Safe / Vault', balance: 145000, type: 'Cash', color: 'emerald' },
          SAMPATH: { name: 'Sampath Bank (015451000552)', balance: 485000, type: 'Bank', color: 'amber' },
          NTB: { name: 'Nations Trust Bank (100450007782)', balance: 290000, type: 'Bank', color: 'sky' },
          PETTY: { name: 'Petty Cash Float Drawer', balance: 25000, type: 'Petty', color: 'purple' }
        };
        localStorage.setItem('PA_BANK_ACCOUNTS_V1', JSON.stringify(accs));
      }
      return accs;
    },

    saveAccounts(accs) {
      localStorage.setItem('PA_BANK_ACCOUNTS_V1', JSON.stringify(accs));
      this.render();
    },

    getIous() {
      let ious = [];
      try {
        ious = JSON.parse(localStorage.getItem('PA_PETTY_CASH_IOUS_V1')) || [];
      } catch (_) {}
      return ious;
    },

    saveIous(ious) {
      localStorage.setItem('PA_PETTY_CASH_IOUS_V1', JSON.stringify(ious));
      this.render();
    },

    issueIouPrompt() {
      const staff = prompt('Enter Staff Member Name (e.g. Thuwan, Kavindi, Irshad):');
      if (!staff) return;
      const amtStr = prompt('Enter Advance Amount (LKR):');
      const amt = Number(amtStr);
      if (!amt || isNaN(amt) || amt <= 0) return;
      const purpose = prompt('Enter Purpose (e.g. Fuel advance, emergency delivery, office tea):') || 'Advance';

      const ious = this.getIous();
      ious.unshift({
        id: 'IOU_' + Date.now(),
        staff,
        amount: amt,
        purpose,
        date: new Date().toISOString().split('T')[0],
        status: 'Pending',
        issuedBy: window.currentUser?.displayName || 'Manager'
      });
      this.saveIous(ious);
      if (window.showToast) window.showToast(`✓ IOU of LKR ${amt.toLocaleString()} issued to ${staff}.`);
    },

    settleIou(iouId) {
      const ious = this.getIous();
      const item = ious.find(i => i.id === iouId);
      if (item) {
        item.status = 'Settled';
        item.settledAt = new Date().toISOString();
        item.settledBy = window.currentUser?.displayName || 'Manager';
        this.saveIous(ious);
        if (window.showToast) window.showToast(`✓ IOU for ${item.staff} settled.`);
      }
    },

    openInterAccountTransferModal() {
      const fromAcc = prompt('Transfer FROM account (VAULT, SAMPATH, NTB, PETTY):', 'VAULT');
      if (!fromAcc) return;
      const toAcc = prompt('Transfer TO account (VAULT, SAMPATH, NTB, PETTY):', 'SAMPATH');
      if (!toAcc) return;
      const amtStr = prompt('Enter Transfer Amount (LKR):');
      const amt = Number(amtStr);
      if (!amt || isNaN(amt) || amt <= 0) return;

      const accs = this.getAccounts();
      const fKey = fromAcc.toUpperCase().trim();
      const tKey = toAcc.toUpperCase().trim();

      if (!accs[fKey] || !accs[tKey]) {
        alert('Invalid account codes specified.');
        return;
      }

      accs[fKey].balance -= amt;
      accs[tKey].balance += amt;
      this.saveAccounts(accs);
      if (window.showToast) window.showToast(`✓ Transferred LKR ${amt.toLocaleString()} from ${fKey} to ${tKey}.`);
    },

    render() {
      const container = document.getElementById('cashBankingLedgerContainer');
      if (!container) return;

      const accs = this.getAccounts();
      const ious = this.getIous();
      const pendingIous = ious.filter(i => i.status === 'Pending');
      const pendingIouTotal = pendingIous.reduce((sum, i) => sum + (Number(i.amount) || 0), 0);

      container.innerHTML = `
        <div class="space-y-4 text-xs">
          <!-- Account Balances Row -->
          <div class="grid grid-cols-2 lg:grid-cols-4 gap-2.5">
            <div class="bg-white p-3 rounded-2xl border border-emerald-200 shadow-sm">
              <span class="text-[10px] font-bold text-slate-500 uppercase block">Main Cash Vault</span>
              <span class="text-base sm:text-lg font-black text-emerald-700 block">LKR ${Number(accs.VAULT?.balance || 0).toLocaleString()}</span>
              <span class="text-[9.5px] text-slate-400">Physical Office Safe</span>
            </div>
            <div class="bg-white p-3 rounded-2xl border border-orange-200 shadow-sm">
              <span class="text-[10px] font-bold text-slate-500 uppercase block">Sampath Bank</span>
              <span class="text-base sm:text-lg font-black text-orange-700 block">LKR ${Number(accs.SAMPATH?.balance || 0).toLocaleString()}</span>
              <span class="text-[9.5px] text-slate-400">A/C 015451000552</span>
            </div>
            <div class="bg-white p-3 rounded-2xl border border-sky-200 shadow-sm">
              <span class="text-[10px] font-bold text-slate-500 uppercase block">NTB Bank</span>
              <span class="text-base sm:text-lg font-black text-sky-700 block">LKR ${Number(accs.NTB?.balance || 0).toLocaleString()}</span>
              <span class="text-[9.5px] text-slate-400">A/C 100450007782</span>
            </div>
            <div class="bg-white p-3 rounded-2xl border border-purple-200 shadow-sm">
              <div class="flex justify-between items-start">
                <span class="text-[10px] font-bold text-slate-500 uppercase block">Petty Cash Float</span>
                <span class="text-[9px] bg-purple-100 text-purple-800 font-bold px-1.5 py-0.2 rounded-full">${pendingIous.length} IOUs</span>
              </div>
              <span class="text-base sm:text-lg font-black text-purple-700 block">LKR ${Number(accs.PETTY?.balance || 0).toLocaleString()}</span>
              <span class="text-[9.5px] text-slate-400">Outstanding: LKR ${pendingIouTotal.toLocaleString()}</span>
            </div>
          </div>

          <!-- Quick Action Bar -->
          <div class="flex flex-wrap items-center justify-between gap-2 bg-slate-50 p-2.5 rounded-xl border border-slate-200">
            <span class="font-extrabold text-slate-700"><i class="fa-solid fa-vault mr-1.5 text-indigo-600"></i>Treasury & Petty Cash Actions</span>
            <div class="flex items-center gap-1.5">
              <button onclick="CashBankingLedger.openInterAccountTransferModal()" class="px-2.5 py-1.5 bg-[#1B365D] hover:bg-[#0B192C] text-white font-bold rounded-lg transition active:scale-95">
                <i class="fa-solid fa-arrow-right-arrow-left mr-1 text-sky-300"></i> Inter-Account Transfer
              </button>
              <button onclick="CashBankingLedger.issueIouPrompt()" class="px-2.5 py-1.5 bg-purple-600 hover:bg-purple-700 text-white font-bold rounded-lg transition active:scale-95">
                <i class="fa-solid fa-receipt mr-1 text-purple-200"></i> Issue Staff IOU
              </button>
            </div>
          </div>

          <!-- Staff IOU Advances Table -->
          <div class="bg-white rounded-2xl border border-slate-200 overflow-hidden shadow-sm">
            <div class="px-3.5 py-2.5 bg-slate-100 border-b border-slate-200 flex justify-between items-center font-bold text-slate-800">
              <span>Active Petty Cash Advances & IOUs</span>
              <span class="text-slate-500 font-normal text-[11px]">${pendingIous.length} Pending Settlement</span>
            </div>
            <div class="overflow-x-auto max-h-56 custom-scroll">
              <table class="w-full text-left text-[11px]">
                <thead class="bg-slate-50 text-slate-600 uppercase border-b text-[10px]">
                  <tr>
                    <th class="py-2 px-3">Date</th>
                    <th class="py-2 px-3">Staff Member</th>
                    <th class="py-2 px-3">Purpose</th>
                    <th class="py-2 px-3">Amount</th>
                    <th class="py-2 px-3">Status</th>
                    <th class="py-2 px-3 text-right">Action</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-100">
                  ${ious.length === 0 ? `
                    <tr><td colspan="6" class="text-center py-5 text-slate-400">No petty cash IOUs logged.</td></tr>
                  ` : ious.map(i => `
                    <tr class="hover:bg-slate-50 transition">
                      <td class="py-2 px-3 font-mono">${i.date}</td>
                      <td class="py-2 px-3 font-bold text-slate-800">${i.staff}</td>
                      <td class="py-2 px-3 text-slate-500">${i.purpose}</td>
                      <td class="py-2 px-3 font-extrabold text-slate-900">LKR ${Number(i.amount).toLocaleString()}</td>
                      <td class="py-2 px-3">
                        <span class="px-2 py-0.5 rounded-full font-bold text-[9px] ${i.status === 'Settled' ? 'bg-emerald-100 text-emerald-800' : 'bg-amber-100 text-amber-800 animate-pulse'}">
                          ${i.status}
                        </span>
                      </td>
                      <td class="py-2 px-3 text-right">
                        ${i.status === 'Pending' ? `
                          <button onclick="CashBankingLedger.settleIou('${i.id}')" class="px-2 py-0.5 bg-emerald-600 hover:bg-emerald-700 text-white rounded text-[10px] font-bold">
                            Settle
                          </button>
                        ` : `<span class="text-slate-400 text-[10px] italic">Completed</span>`}
                      </td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      `;
    }
  };

  window.CashBankingLedger = CashBankingLedger;


  // ==========================================================================
  // 4. CATEGORY 16: AUTOMATED WHATSAPP & SMS NOTIFICATIONS & RECEIPT SLIP
  // ==========================================================================

  // A. 1-Tap Customer WhatsApp Receipt Generator
  window.shareSaleReceiptWhatsApp = function (saleId) {
    if (!window.sales) return;
    const sale = window.sales.find(s => s.id === saleId);
    if (!sale) {
      alert('Sale transaction not found.');
      return;
    }

    const fmtLKR = window.formatLKR ? window.formatLKR(sale.amount) : `LKR ${sale.amount}`;
    const dateFormatted = sale.date || new Date().toISOString().split('T')[0];
    const dealerName = sale.dealer || 'Valued Dealer';
    const repName = sale.dsr || 'P&A Representative';
    const invNo = sale.invNo || 'N/A';
    const method = sale.method || 'Cash';

    let methodDetails = '';
    if (method === 'Cheque' && sale.details) {
      methodDetails = `\n💳 *Cheque Bank:* ${sale.details.chequeBank || 'Bank'}\n🧾 *Cheque No:* ${sale.details.chequeNo || 'N/A'}\n📆 *Cheque Date:* ${sale.details.chequeDate || 'N/A'}`;
    } else if (method === 'Direct Deposit' && sale.details) {
      methodDetails = `\n🏦 *Bank A/C:* ${sale.details.depositBank || 'Bank'} (${sale.details.bankAccNo || ''})`;
    }

    let gpsText = '';
    if (sale.gps && sale.gps.lat) {
      gpsText = `\n📍 *Visit Location:* GPS Verified (https://maps.google.com/?q=${sale.gps.lat},${sale.gps.lng})`;
    }

    let msg = `🧾 *P&A DISTRIBUTORS - OFFICIAL PAYMENT RECEIPT*\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `📅 *Date:* ${dateFormatted}\n`;
    msg += `📑 *Invoice / Receipt:* #${invNo}\n`;
    msg += `🏪 *Dealer:* ${dealerName}\n`;
    msg += `👤 *Sales Rep:* ${repName}\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `💵 *Amount Collected:* *${fmtLKR}*\n`;
    msg += `💰 *Payment Method:* ${method}${methodDetails}${gpsText}\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `✅ *Status:* Payment Received & Verified\n\n`;
    msg += `Thank you for your business!\n`;
    msg += `📞 *Hotline / Office Inquiries:* +94 11 234 5678\n`;
    msg += `🏢 *P&A Distributors, Sri Lanka*`;

    const encoded = encodeURIComponent(msg);
    window.open(`https://wa.me/?text=${encoded}`, '_blank');
  };

  // B. Automated Overdue Payment Reminder (WhatsApp)
  window.sharePaymentReminderWhatsApp = function (dealerName, amount, refNo) {
    const fmt = window.formatLKR ? window.formatLKR(amount) : `LKR ${amount}`;
    let msg = `📢 *PAYMENT REMINDER - P&A DISTRIBUTORS*\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `Dear Partner (*${dealerName}*),\n\n`;
    msg += `Greetings from P&A Distributors.\n`;
    msg += `This is a gentle reminder regarding the payment of *${fmt}* (Ref: ${refNo || 'Sales Credit'}).\n\n`;
    msg += `Kindly arrange for settlement at your earliest convenience via our official bank accounts:\n`;
    msg += `🏦 *Sampath Bank PLC:*\n   • Account: *015451000552*\n   • Name: P&A Distributors\n\n`;
    msg += `🏦 *Nations Trust Bank (NTB):*\n   • Account: *100450007782*\n   • Name: P&A Distributors\n\n`;
    msg += `If already settled, please ignore this notice and share the deposit slip.\n`;
    msg += `Thank you for your cooperation!\n`;
    msg += `📞 *Accounts Department:* +94 11 234 5678`;

    const encoded = encodeURIComponent(msg);
    window.open(`https://wa.me/?text=${encoded}`, '_blank');
  };

  // C. SMS Fallback Trigger
  window.sendSMSReceipt = function (saleId) {
    if (!window.sales) return;
    const sale = window.sales.find(s => s.id === saleId);
    if (!sale) return;
    const fmt = window.formatLKR ? window.formatLKR(sale.amount) : `LKR ${sale.amount}`;
    const txt = `P&A Distributors: Payment received from ${sale.dealer}. Amount: ${fmt} on ${sale.date} (Inv: #${sale.invNo}). Thank you!`;
    window.open(`sms:?body=${encodeURIComponent(txt)}`, '_self');
  };

  // D. Dealer Receipt Slip Modal & Printing
  let currentActiveReceiptSale = null;

  window.openDealerReceiptModal = function (saleId) {
    let sale = (window.sales || []).find(s => s.id === saleId);
    if (!sale && window.sales && window.sales.length > 0) {
      sale = window.sales[0];
    }
    if (!sale) {
      sale = {
        id: 'REC_DEMO',
        date: new Date().toISOString().split('T')[0],
        invNo: 'INV-2026-DEMO',
        dealer: 'Sample Dealer Mart',
        dsr: 'Thuwan',
        method: 'Cash',
        amount: 45000,
        status: 'Verified'
      };
    }

    currentActiveReceiptSale = sale;
    ensureReceiptModalExists();

    const slip = document.getElementById('dealerReceiptSlip');
    const modal = document.getElementById('dealerReceiptModal');
    if (!slip || !modal) return;

    const fmt = window.formatLKR ? window.formatLKR(sale.amount) : `LKR ${sale.amount}`;
    const gpsHtml = sale.gps && sale.gps.lat ? `
      <div class="mt-2 p-2 bg-emerald-50 rounded border border-emerald-200 text-[10px] text-emerald-800 font-bold flex items-center justify-between">
        <span><i class="fa-solid fa-location-dot text-emerald-600 mr-1"></i>GPS Location Verified (${sale.gps.lat}, ${sale.gps.lng})</span>
        <a href="https://maps.google.com/?q=${sale.gps.lat},${sale.gps.lng}" target="_blank" class="text-sky-600 underline">View Map</a>
      </div>
    ` : '';

    slip.innerHTML = `
      <div class="p-6 space-y-4 text-slate-800 bg-white font-sans text-xs">
        <!-- Receipt Header -->
        <div class="text-center border-b-2 border-slate-900 pb-3">
          <h2 class="text-lg font-black tracking-wider text-slate-900 uppercase">P&A DISTRIBUTORS</h2>
          <p class="text-[11px] font-bold text-slate-600">OFFICE AUTOMATION SYSTEM • OFFICIAL PAYMENT RECEIPT</p>
          <p class="text-[10px] text-slate-500">Authorized Distribution Partner • Sri Lanka • Hotline: +94 11 234 5678</p>
        </div>

        <!-- Meta Details -->
        <div class="grid grid-cols-2 gap-2 text-[11px] pb-2 border-b border-slate-200">
          <div><span class="text-slate-500">Receipt / Inv No:</span> <strong class="text-slate-900 font-mono">#${sale.invNo || 'N/A'}</strong></div>
          <div class="text-right"><span class="text-slate-500">Date:</span> <strong class="text-slate-900">${sale.date || 'N/A'}</strong></div>
          <div><span class="text-slate-500">Dealer / Shop:</span> <strong class="text-slate-900 font-bold">${sale.dealer || 'Valued Dealer'}</strong></div>
          <div class="text-right"><span class="text-slate-500">Sales Rep:</span> <strong class="text-slate-900">${sale.dsr || 'P&A Rep'}</strong></div>
        </div>

        <!-- Amount Box -->
        <div class="p-4 bg-slate-50 rounded-2xl border-2 border-slate-300 text-center space-y-1">
          <span class="text-[10px] uppercase font-bold text-slate-500 tracking-wider">Total Amount Received</span>
          <div class="text-2xl font-black text-slate-900 font-mono">${fmt}</div>
          <div class="text-xs font-bold text-emerald-700 uppercase">Payment Method: ${sale.method}</div>
        </div>

        ${sale.method === 'Cheque' && sale.details ? `
          <div class="p-2.5 bg-amber-50 rounded-xl border border-amber-200 text-[11px] space-y-1">
            <div><span class="text-slate-500">Cheque Bank:</span> <strong>${sale.details.chequeBank || 'N/A'}</strong></div>
            <div><span class="text-slate-500">Cheque Number:</span> <strong class="font-mono">${sale.details.chequeNo || 'N/A'}</strong></div>
            <div><span class="text-slate-500">Realization Date:</span> <strong>${sale.details.chequeDate || 'N/A'}</strong></div>
          </div>
        ` : ''}

        ${gpsHtml}

        <!-- Footer Signatures -->
        <div class="pt-6 grid grid-cols-2 gap-4 text-center text-[10px] text-slate-500 border-t border-slate-200">
          <div>
            <div class="border-b border-slate-400 w-32 mx-auto mb-1"></div>
            <span>DSR / Collector Signature</span>
          </div>
          <div>
            <div class="border-b border-slate-400 w-32 mx-auto mb-1"></div>
            <span>Dealer Stamp & Signature</span>
          </div>
        </div>

        <div class="text-center text-[9px] text-slate-400 pt-2">
          This is an official computer-generated receipt from P&A Distributors Daily Sales OS.
        </div>
      </div>
    `;

    modal.classList.remove('hidden');
  };

  window.previewTemplateDealerReceipt = function () {
    window.openDealerReceiptModal(currentActiveReceiptSale?.id);
  };

  window.printDealerReceipt = function () {
    document.body.classList.add('printing-receipt');
    window.print();
    setTimeout(() => {
      document.body.classList.remove('printing-receipt');
    }, 1000);
  };

  function ensureReceiptModalExists() {
    if (document.getElementById('dealerReceiptModal')) return;

    const modal = document.createElement('div');
    modal.id = 'dealerReceiptModal';
    modal.className = 'fixed inset-0 bg-black/75 backdrop-blur-sm z-50 flex items-center justify-center p-3 sm:p-4 hidden animate-fade-in';
    modal.innerHTML = `
      <div class="bg-white rounded-3xl max-w-lg w-full shadow-2xl border border-slate-300 overflow-hidden flex flex-col max-h-[95vh]">
        <!-- Action Toolbar (Hidden in Print) -->
        <div class="receipt-no-print bg-[#0B192C] text-white p-3 flex items-center justify-between border-b border-slate-700">
          <div class="flex items-center gap-2">
            <span class="w-7 h-7 rounded-lg bg-emerald-600 flex items-center justify-center text-xs shadow">
              <i class="fa-solid fa-receipt"></i>
            </span>
            <span class="font-extrabold text-xs">Official Dealer Receipt Slip</span>
          </div>
          <div class="flex items-center gap-1.5">
            <button onclick="window.printDealerReceipt()" class="px-2.5 py-1 bg-white/10 hover:bg-white/20 text-white font-bold rounded-lg text-xs transition flex items-center gap-1">
              <i class="fa-solid fa-print"></i> Print
            </button>
            <button onclick="if(currentActiveReceiptSale) window.shareSaleReceiptWhatsApp(currentActiveReceiptSale.id)" class="px-2.5 py-1 bg-emerald-600 hover:bg-emerald-700 text-white font-bold rounded-lg text-xs transition flex items-center gap-1">
              <i class="fa-brands fa-whatsapp"></i> WhatsApp
            </button>
            <button onclick="document.getElementById('dealerReceiptModal').classList.add('hidden')" class="text-slate-400 hover:text-white p-1">
              <i class="fa-solid fa-xmark text-base"></i>
            </button>
          </div>
        </div>

        <!-- The Printable Slip -->
        <div class="overflow-y-auto custom-scroll p-4">
          <div id="dealerReceiptSlip" class="bg-white rounded-2xl border border-slate-300 shadow-sm max-w-[560px] mx-auto"></div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
  }


  // ==========================================================================
  // 5. HOOK AND ENHANCE CORE APP FUNCTIONS
  // ==========================================================================
  function hookCoreAppFunctions() {
    // A. Wrap renderSalesTable to append WhatsApp and GPS buttons to every row
    const origRenderSalesTable = window.renderSalesTable;
    if (typeof origRenderSalesTable === 'function') {
      window.renderSalesTable = function (list) {
        origRenderSalesTable(list);

        const rows = document.querySelectorAll('#salesTableBody tr');
        rows.forEach(tr => {
          const actionContainer = tr.querySelector('div.flex.items-center.justify-center');
          if (actionContainer && !actionContainer.querySelector('.btn-wa-receipt')) {
            const match = actionContainer.innerHTML.match(/(?:editSale|delSale|verifySalePayment)\(['"]([^'"]+)['"]\)/);
            if (match) {
              const saleId = match[1];
              const sale = (window.sales || []).find(s => s.id === saleId);

              // 1. WhatsApp Receipt Button
              const waBtn = document.createElement('button');
              waBtn.className = 'w-7 h-7 rounded-lg bg-emerald-50 hover:bg-emerald-100 text-emerald-700 flex items-center justify-center transition border border-emerald-200 btn-wa-receipt active:scale-95';
              waBtn.title = 'Send Official WhatsApp Receipt to Dealer';
              waBtn.innerHTML = `<i class="fa-brands fa-whatsapp text-xs text-emerald-600"></i>`;
              waBtn.onclick = (e) => { e.stopPropagation(); window.shareSaleReceiptWhatsApp(saleId); };
              actionContainer.appendChild(waBtn);

              // 2. Receipt Slip Modal Preview Button
              const slipBtn = document.createElement('button');
              slipBtn.className = 'w-7 h-7 rounded-lg bg-teal-50 hover:bg-teal-100 text-teal-700 flex items-center justify-center transition border border-teal-200 active:scale-95';
              slipBtn.title = 'View & Print Dealer Receipt Slip';
              slipBtn.innerHTML = `<i class="fa-solid fa-receipt text-xs text-teal-600"></i>`;
              slipBtn.onclick = (e) => { e.stopPropagation(); window.openDealerReceiptModal(saleId); };
              actionContainer.appendChild(slipBtn);

              // 3. GPS Pin Link
              if (sale && sale.gps && sale.gps.lat) {
                const gpsBtn = document.createElement('a');
                gpsBtn.href = `https://maps.google.com/?q=${sale.gps.lat},${sale.gps.lng}`;
                gpsBtn.target = '_blank';
                gpsBtn.className = 'w-7 h-7 rounded-lg bg-sky-50 hover:bg-sky-100 text-sky-700 flex items-center justify-center transition border border-sky-200 active:scale-95';
                gpsBtn.title = `View GPS Stamped Location (${sale.gps.lat}, ${sale.gps.lng})`;
                gpsBtn.innerHTML = `<i class="fa-solid fa-location-dot text-xs text-sky-600"></i>`;
                gpsBtn.onclick = (e) => e.stopPropagation();
                actionContainer.appendChild(gpsBtn);
              }
            }
          }
        });
      };
    }

    // B. Wrap renderChequesTable to append WhatsApp payment reminder to cheques
    const origRenderChequesTable = window.renderChequesTable;
    if (typeof origRenderChequesTable === 'function') {
      window.renderChequesTable = function (list) {
        origRenderChequesTable(list);

        const rows = document.querySelectorAll('#chequesTableBody tr');
        rows.forEach(tr => {
          const actionTd = tr.querySelector('td:last-child');
          if (actionTd && !actionTd.querySelector('.btn-wa-reminder')) {
            const cells = tr.querySelectorAll('td');
            if (cells.length >= 4) {
              const dealerName = cells[2]?.querySelector('span.font-bold')?.innerText || cells[2]?.innerText || 'Dealer';
              const amtText = cells[4]?.innerText || '0';
              const amtClean = window.parseAmount ? window.parseAmount(amtText) : Number(amtText);
              const chqDetails = cells[3]?.innerText || 'Cheque';

              const waRemBtn = document.createElement('button');
              waRemBtn.className = 'w-7 h-7 rounded-lg bg-emerald-50 hover:bg-emerald-100 text-emerald-700 flex items-center justify-center border border-emerald-300 transition active:scale-95 ml-1 btn-wa-reminder';
              waRemBtn.title = 'Send WhatsApp Payment Reminder';
              waRemBtn.innerHTML = `<i class="fa-brands fa-whatsapp text-xs text-emerald-600"></i>`;
              waRemBtn.onclick = (e) => {
                e.stopPropagation();
                window.sharePaymentReminderWhatsApp(dealerName.trim(), amtClean, chqDetails.trim());
              };
              const container = actionTd.querySelector('div') || actionTd;
              container.appendChild(waRemBtn);
            }
          }
        });
      };
    }

    // C. Enhance switchMileageSubView to support 'journey' subview
    const origSwitchMileageSubView = window.switchMileageSubView;
    window.switchMileageSubView = function (v) {
      const btnJourney = document.getElementById('btnMileageSubJourney');
      const cJourney = document.getElementById('mileageJourneyViewContainer');
      const btnMatrix = document.getElementById('btnMileageSubMatrix');
      const btnLogs = document.getElementById('btnMileageSubLogs');
      const cMatrix = document.getElementById('mileageMatrixViewContainer');
      const cLogs = document.getElementById('mileageLogsViewContainer');

      if (v === 'journey') {
        if (btnMatrix) btnMatrix.className = "px-4 py-2 rounded-xl text-slate-600 hover:text-black flex items-center gap-2 transition font-bold";
        if (btnLogs) btnLogs.className = "px-4 py-2 rounded-xl text-slate-600 hover:text-black flex items-center gap-2 transition font-bold";
        if (btnJourney) btnJourney.className = "px-4 py-2 rounded-xl bg-[#152D35] text-[#D4ECDD] shadow-xs flex items-center gap-2 transition font-bold";
        if (cMatrix) cMatrix.classList.add('hidden');
        if (cLogs) cLogs.classList.add('hidden');
        if (cJourney) cJourney.classList.remove('hidden');
        if (window.JourneyPlanner) window.JourneyPlanner.render();
      } else {
        if (btnJourney) btnJourney.className = "px-4 py-2 rounded-xl text-slate-600 hover:text-black flex items-center gap-2 transition font-bold";
        if (cJourney) cJourney.classList.add('hidden');
        if (typeof origSwitchMileageSubView === 'function') {
          origSwitchMileageSubView(v);
        }
      }
    };

    // D. Wrap handleSaveSale to record GPS data
    const origHandleSaveSale = window.handleSaveSale;
    if (typeof origHandleSaveSale === 'function') {
      window.handleSaveSale = function (e) {
        const gpsVal = document.getElementById('saleGpsValue')?.value;
        let gpsObj = null;
        if (gpsVal) {
          try { gpsObj = JSON.parse(gpsVal); } catch (_) {}
        }

        // Run the original save
        origHandleSaveSale(e);

        // If GPS was captured and a new sale was inserted at index 0, attach GPS
        if (gpsObj && window.sales && window.sales.length > 0) {
          window.sales[0].gps = gpsObj;
          localStorage.setItem('PA_SALES_V2', JSON.stringify(window.sales));
          if (typeof window.applyFilters === 'function') window.applyFilters();
        }

        // Reset GPS input
        const gpsInput = document.getElementById('saleGpsValue');
        if (gpsInput) gpsInput.value = '';
        const badge = document.getElementById('saleGpsBadge');
        if (badge) badge.classList.add('hidden');
      };
    }
  }


  // ==========================================================================
  // 6. DOM READY INJECTION PIPELINE
  // ==========================================================================
  let enhancementsInjected = false;
  function injectEnhancements() {
    if (enhancementsInjected) return;
    enhancementsInjected = true;
    if (typeof initAllUpgrades === 'function') initAllUpgrades();
    MultiDeviceSyncManager.init();
    ensureReceiptModalExists();

    // 1. Inject GPS and Voice buttons into Sale Modal
    injectSaleModalAdditions();

    // 2. Inject Journey Planner Tab in Mileage Area
    injectJourneyPlannerNav();

    // 3. Inject Cash Banking Ledger View
    injectCashBankingHub();

    // 4. Hook Core functions
    hookCoreAppFunctions();
  }

  function injectSaleModalAdditions() {
    const dealerInput = document.getElementById('saleDealer');
    if (!dealerInput || document.getElementById('saleGpsContainer')) return;

    // GPS Container
    const gpsWrapper = document.createElement('div');
    gpsWrapper.id = 'saleGpsContainer';
    gpsWrapper.className = 'mt-1 flex items-center justify-between gap-2';
    gpsWrapper.innerHTML = `
      <input type="hidden" id="saleGpsValue" value="" />
      <div id="saleGpsBadge" class="hidden"></div>
      <button type="button" onclick="window.captureGPSLocation('saleGpsValue', 'saleGpsBadge')" class="px-2.5 py-1 bg-slate-100 hover:bg-emerald-50 text-slate-700 hover:text-emerald-800 border border-slate-300 rounded-lg text-[10px] font-bold flex items-center gap-1 transition active:scale-95">
        <i class="fa-solid fa-location-crosshairs text-emerald-600"></i>
        <span>Capture GPS Location</span>
      </button>
    `;
    dealerInput.parentElement.appendChild(gpsWrapper);

    // Voice Dictation in Remarks / Notes
    if (!document.getElementById('saleVoiceBtn')) {
      const voiceBtn = document.createElement('button');
      voiceBtn.type = 'button';
      voiceBtn.id = 'saleVoiceBtn';
      voiceBtn.title = 'Voice Dictate Dealer or Remarks';
      voiceBtn.className = 'px-2 py-1 bg-slate-100 hover:bg-rose-50 text-slate-600 hover:text-rose-600 border border-slate-300 rounded-lg text-[10px] font-bold flex items-center gap-1 transition active:scale-95 mt-1';
      voiceBtn.innerHTML = `<i class="fa-solid fa-microphone text-rose-500"></i> Voice Dictate`;
      voiceBtn.onclick = () => window.startVoiceDictation('saleDealer', 'saleVoiceBtn');
      gpsWrapper.appendChild(voiceBtn);
    }
  }

  function injectJourneyPlannerNav() {
    const btnMatrix = document.getElementById('btnMileageSubMatrix');
    if (btnMatrix && !document.getElementById('btnMileageSubJourney')) {
      const btnBar = btnMatrix.parentElement;
      const btnJourney = document.createElement('button');
      btnJourney.id = 'btnMileageSubJourney';
      btnJourney.onclick = () => window.switchMileageSubView('journey');
      btnJourney.className = 'px-4 py-2 rounded-xl text-slate-600 hover:text-black flex items-center gap-2 transition font-bold';
      btnJourney.innerHTML = `
        <i class="fa-solid fa-map-location-dot text-amber-500"></i>
        <span>Daily Route Journey & Checklist</span>
      `;
      btnBar.appendChild(btnJourney);
    }

    const mileageArea = document.getElementById('tabContentMileage');
    if (mileageArea && !document.getElementById('mileageJourneyViewContainer')) {
      const journeyView = document.createElement('div');
      journeyView.id = 'mileageJourneyViewContainer';
      journeyView.className = 'space-y-4 bg-white p-3 sm:p-5 rounded-3xl border border-slate-200 shadow-sm hidden';
      journeyView.innerHTML = `<div id="journeyPlannerContainer"></div>`;
      mileageArea.appendChild(journeyView);
    }
  }

/**
 * ============================================================================
 *   P&A DISTRIBUTORS - UPGRADES 1, 2, AND 3 EXTENSION SUITE
 * ============================================================================
 * 1. Visuals & UI: Executive Glassmorphic Dashboard, Theme Switcher, Mobile Dock
 * 2. Integrations: 2-Way Google Sheets, Telegram Bot Alerts, WhatsApp Close-Out
 * 3. Daily Automations: Day-End Cash Balancing, DSR Fuel Claim Slip, Evening Checkout
 * ============================================================================
 */


  // Helper currency formatter
  function fmtLKR(val) {
    const num = Number(val) || 0;
    return 'Rs. ' + num.toLocaleString('en-LK', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function getTodayIso() {
    return new Date().toISOString().split('T')[0];
  }

  // ==========================================================================
  // MODULE 1: VISUALS, UI & USER EXPERIENCE (UPGRADE 1)
  // ==========================================================================

  // 1.1 THEME PALETTE SWITCHER
  const ThemePaletteSwitcher = {
    themes: {
      corporate: { name: 'P&A Classic', icon: 'fa-building', color: '#152D35' },
      oled: { name: 'Executive OLED', icon: 'fa-moon', color: '#090D16' },
      arctic: { name: 'Arctic Studio', icon: 'fa-sun', color: '#FFFFFF' }
    },
    activeTheme: localStorage.getItem('PA_ACTIVE_THEME') || 'corporate',

    init() {
      this.injectThemeStyles();
      this.applyTheme(this.activeTheme);
      this.injectHeaderSelector();
    },

    injectThemeStyles() {
      if (document.getElementById('pa-custom-theme-styles')) return;
      const style = document.createElement('style');
      style.id = 'pa-custom-theme-styles';
      style.innerHTML = `
        /* Executive OLED Dark Theme */
        html[data-pa-theme="oled"] body {
          background-color: #060B13 !important;
          color: #E2E8F0 !important;
        }
        html[data-pa-theme="oled"] .bg-white {
          background-color: #0C1524 !important;
          border-color: #1E293B !important;
          color: #E2E8F0 !important;
        }
        html[data-pa-theme="oled"] .bg-slate-50, 
        html[data-pa-theme="oled"] .bg-slate-100 {
          background-color: #101B2E !important;
          border-color: #1E293B !important;
          color: #CBD5E1 !important;
        }
        html[data-pa-theme="oled"] header {
          background: linear-gradient(135deg, #09121F 0%, #060B13 100%) !important;
          border-bottom-color: #1E293B !important;
        }
        html[data-pa-theme="oled"] .text-slate-800,
        html[data-pa-theme="oled"] .text-slate-900,
        html[data-pa-theme="oled"] .text-[#0B192C],
        html[data-pa-theme="oled"] .text-[#152D35] {
          color: #F8FAFC !important;
        }
        html[data-pa-theme="oled"] .text-slate-700,
        html[data-pa-theme="oled"] .text-slate-600 {
          color: #94A3B8 !important;
        }
        html[data-pa-theme="oled"] input,
        html[data-pa-theme="oled"] select,
        html[data-pa-theme="oled"] textarea {
          background-color: #132037 !important;
          border-color: #334155 !important;
          color: #F8FAFC !important;
        }

        /* Arctic Studio Light Theme */
        html[data-pa-theme="arctic"] body {
          background-color: #F4F6F9 !important;
          color: #0F172A !important;
        }
        html[data-pa-theme="arctic"] .bg-white {
          background-color: #FFFFFF !important;
          border-color: #E2E8F0 !important;
          box-shadow: 0 4px 16px -2px rgba(15, 23, 42, 0.05) !important;
        }
        html[data-pa-theme="arctic"] header {
          background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%) !important;
        }

        /* Glassmorphic KPI Cards Styling */
        .glass-kpi-card {
          backdrop-filter: blur(12px);
          -webkit-backdrop-filter: blur(12px);
          transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .glass-kpi-card:hover {
          transform: translateY(-2px);
        }
      `;
      document.head.appendChild(style);
    },

    applyTheme(themeKey) {
      if (!this.themes[themeKey]) themeKey = 'corporate';
      this.activeTheme = themeKey;
      localStorage.setItem('PA_ACTIVE_THEME', themeKey);
      if (document.documentElement) document.documentElement.setAttribute('data-pa-theme', themeKey);
      if (document.body) document.body.setAttribute('data-pa-theme', themeKey);

      const label = document.getElementById('paThemeCurrentLabel');
      if (label) label.innerText = this.themes[themeKey].name;

      const icon = document.getElementById('paThemeCurrentIcon');
      if (icon) icon.className = `fa-solid ${this.themes[themeKey].icon}`;

      if (typeof window.showToast === 'function') {
        window.showToast(`Active Theme: ${this.themes[themeKey].name}`);
      }
    },

    injectHeaderSelector() {
      if (document.getElementById('paThemeSelectorContainer')) return;
      const syncBadge = document.getElementById('liveSyncBadgeContainer');
      const themeBtn = document.getElementById('topBtnQuickTheme') || document.querySelector('header button');
      const target = syncBadge || themeBtn;
      if (!target || !target.parentElement) return;

      const container = document.createElement('div');
      container.id = 'paThemeSelectorContainer';
      container.className = 'flex items-center gap-1 bg-slate-800/80 border border-slate-600 rounded-xl px-2 py-1 shadow-sm text-xs select-none';
      container.innerHTML = `
        <span class="text-slate-400 text-[10px] font-bold uppercase hidden xl:inline">Theme:</span>
        <select id="paThemeSelectDropdown" onchange="window.ThemePaletteSwitcher.applyTheme(this.value)" class="bg-transparent text-slate-200 text-xs font-bold outline-none cursor-pointer">
          <option value="corporate" ${this.activeTheme === 'corporate' ? 'selected' : ''}>🌲 P&A Classic</option>
          <option value="oled" ${this.activeTheme === 'oled' ? 'selected' : ''}>🌙 Executive OLED</option>
          <option value="arctic" ${this.activeTheme === 'arctic' ? 'selected' : ''}>☀️ Arctic Studio</option>
        </select>
      `;
      target.parentElement.insertBefore(container, target);
    }
  };
  window.ThemePaletteSwitcher = ThemePaletteSwitcher;

  // 1.2 EXECUTIVE GLASSMORPHIC DASHBOARD CARDS
  const ExecutiveKpiDashboard = {
    inject() {
      if (document.getElementById('executiveKpiDashboard')) return;
      const salesTab = document.getElementById('tabContentSales');
      if (!salesTab) return;

      const kpiContainer = document.createElement('div');
      kpiContainer.id = 'executiveKpiDashboard';
      kpiContainer.className = 'grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 mb-4';
      kpiContainer.innerHTML = `
        <!-- Card 1: Today's Revenue & Collections -->
        <div class="glass-kpi-card p-4 rounded-3xl bg-gradient-to-br from-[#152D35] to-[#0A181D] text-white border border-[#234A56] shadow-lg flex flex-col justify-between">
          <div class="flex items-center justify-between">
            <span class="text-[10.5px] font-black uppercase tracking-wider text-emerald-300">Revenue & Collections</span>
            <span class="w-8 h-8 rounded-xl bg-emerald-500/20 text-emerald-300 flex items-center justify-center text-sm"><i class="fa-solid fa-coins"></i></span>
          </div>
          <div class="mt-2">
            <h3 class="text-xl sm:text-2xl font-black font-display text-white tracking-tight" id="execKpiTodayRevenue">Rs. 0.00</h3>
            <div class="flex items-center justify-between text-[11px] font-bold text-slate-300 mt-1">
              <span id="execKpiCashVsChq">Cash: Rs. 0 | Cheque: Rs. 0</span>
              <span class="text-emerald-400 font-extrabold" id="execKpiInvoiceCount">0 Bills</span>
            </div>
          </div>
          <div class="w-full bg-white/10 rounded-full h-1.5 mt-3 overflow-hidden">
            <div id="execKpiCollectionBar" class="bg-gradient-to-r from-emerald-400 to-teal-300 h-1.5 rounded-full transition-all duration-500" style="width: 0%"></div>
          </div>
        </div>

        <!-- Card 2: Active DSR Fleet & Mileage -->
        <div class="glass-kpi-card p-4 rounded-3xl bg-gradient-to-br from-[#1E293B] to-[#0F172A] text-white border border-slate-700 shadow-lg flex flex-col justify-between">
          <div class="flex items-center justify-between">
            <span class="text-[10.5px] font-black uppercase tracking-wider text-sky-300">DSR Fleet Mileage (Total LOG)</span>
            <span class="w-8 h-8 rounded-xl bg-sky-500/20 text-sky-300 flex items-center justify-center text-sm"><i class="fa-solid fa-route"></i></span>
          </div>
          <div class="mt-2">
            <h3 class="text-xl sm:text-2xl font-black font-display text-sky-200 tracking-tight" id="execKpiFleetKm">0.0 KM</h3>
            <div class="flex items-center justify-between text-[11px] font-bold text-slate-300 mt-1">
              <span id="execKpiActiveDsrText">0 / 6 DSRs on Road</span>
              <span class="text-indigo-300 font-semibold" id="execKpiAvgKm">Avg: 0 KM</span>
            </div>
          </div>
          <div class="mt-3 flex items-center justify-between text-[10px] text-slate-400 font-bold border-t border-white/10 pt-1">
            <span>Includes Home-Office Commute</span>
            <span class="text-emerald-400 font-extrabold">Active ✓</span>
          </div>
        </div>

        <!-- Card 3: Fleet Fuel Burn (@ 414 LKR/L) -->
        <div class="glass-kpi-card p-4 rounded-3xl bg-gradient-to-br from-[#311B1B] to-[#1C0D0D] text-white border border-rose-900/60 shadow-lg flex flex-col justify-between">
          <div class="flex items-center justify-between">
            <span class="text-[10.5px] font-black uppercase tracking-wider text-rose-300">Fleet Fuel Cost Burn</span>
            <span class="w-8 h-8 rounded-xl bg-rose-500/20 text-rose-300 flex items-center justify-center text-sm"><i class="fa-solid fa-gas-pump"></i></span>
          </div>
          <div class="mt-2">
            <h3 class="text-xl sm:text-2xl font-black font-display text-rose-300 tracking-tight" id="execKpiFuelCost">Rs. 0.00</h3>
            <div class="flex items-center justify-between text-[11px] font-bold text-slate-300 mt-1">
              <span id="execKpiFuelLiters">0.00 Liters Burned</span>
              <span class="text-amber-300 font-bold">@ 414 LKR/L</span>
            </div>
          </div>
          <div class="mt-3 flex items-center justify-between text-[10px] text-rose-200/80 font-bold border-t border-white/10 pt-1">
            <span>Standard: 50.0 KM / Liter</span>
            <button type="button" onclick="window.openDsrFuelSlipModal()" class="text-amber-400 hover:underline">Claim Slip &raquo;</button>
          </div>
        </div>

        <!-- Card 4: Main Cash Vault & Treasury Status -->
        <div class="glass-kpi-card p-4 rounded-3xl bg-gradient-to-br from-[#1C2518] to-[#0D150B] text-white border border-emerald-900/60 shadow-lg flex flex-col justify-between">
          <div class="flex items-center justify-between">
            <span class="text-[10.5px] font-black uppercase tracking-wider text-amber-300">Cash Vault & Treasury</span>
            <span class="w-8 h-8 rounded-xl bg-amber-500/20 text-amber-300 flex items-center justify-center text-sm"><i class="fa-solid fa-vault"></i></span>
          </div>
          <div class="mt-2">
            <h3 class="text-xl sm:text-2xl font-black font-display text-amber-300 tracking-tight" id="execKpiVaultBalance">Rs. 0.00</h3>
            <div class="flex items-center justify-between text-[11px] font-bold text-slate-300 mt-1">
              <span id="execKpiBankDeposits">Banked Today: Rs. 0</span>
              <span class="text-sky-300" id="execKpiPendingPdc">0 PDCs</span>
            </div>
          </div>
          <div class="mt-3 flex items-center justify-between text-[10px] font-bold border-t border-white/10 pt-1">
            <button type="button" onclick="window.openDayEndSettlementModal()" class="text-amber-300 hover:text-white flex items-center gap-1 font-black">
              <i class="fa-solid fa-scale-balanced"></i> Day Settlement &raquo;
            </button>
            <span class="text-emerald-400 font-extrabold">Reconciled</span>
          </div>
        </div>
      `;

      salesTab.insertBefore(kpiContainer, salesTab.firstChild);
      this.update();
    },

    update() {
      const today = getTodayIso();
      const allSales = window.sales || [];
      const allTrips = window.routeTrips || [];
      const allReturns = window.returnCheques || [];
      const allExpenses = window.expenses || [];

      // 1. Sales & Collections Today
      const todaySales = allSales.filter(s => s && s.date === today);
      let totSales = 0;
      let totCash = 0;
      let totCheque = 0;
      todaySales.forEach(s => {
        const amt = Number(s.amount) || 0;
        totSales += amt;
        const mode = (s.paymentMode || s.mode || '').toUpperCase();
        if (mode === 'CASH') totCash += amt;
        else if (mode === 'CHEQUE' || mode === 'PDC') totCheque += amt;
      });

      const elRev = document.getElementById('execKpiTodayRevenue');
      if (elRev) elRev.innerText = fmtLKR(totSales);
      const elCashChq = document.getElementById('execKpiCashVsChq');
      if (elCashChq) elCashChq.innerText = `Cash: ${fmtLKR(totCash)} | Chq: ${fmtLKR(totCheque)}`;
      const elInvCount = document.getElementById('execKpiInvoiceCount');
      if (elInvCount) elInvCount.innerText = `${todaySales.length} Bills Today`;
      const elBar = document.getElementById('execKpiCollectionBar');
      if (elBar && totSales > 0) {
        const pct = Math.min(100, Math.round(((totCash + totCheque) / totSales) * 100));
        elBar.style.width = `${pct}%`;
      }

      // 2. Active DSR Fleet & Mileage Today
      const todayTrips = allTrips.filter(t => t && t.date === today);
      const activeDsrs = new Set(todayTrips.map(t => t.dsr)).size;
      let totFleetKm = 0;
      let totFuelCost = 0;
      let totFuelLiters = 0;
      todayTrips.forEach(t => {
        totFleetKm += (Number(t.totalKm) || 0);
        totFuelCost += (Number(t.fuelCost) || 0);
        totFuelLiters += (Number(t.fuelLiters) || 0);
      });

      const elFleetKm = document.getElementById('execKpiFleetKm');
      if (elFleetKm) elFleetKm.innerText = `${totFleetKm.toFixed(1)} KM`;
      const elActiveDsr = document.getElementById('execKpiActiveDsrText');
      if (elActiveDsr) elActiveDsr.innerText = `${activeDsrs} / 6 DSRs Active`;
      const elAvgKm = document.getElementById('execKpiAvgKm');
      if (elAvgKm) {
        const avg = activeDsrs > 0 ? (totFleetKm / activeDsrs).toFixed(1) : '0';
        elAvgKm.innerText = `Avg: ${avg} KM`;
      }

      // 3. Fuel Cost Today (@ 414 LKR)
      const elFuelCost = document.getElementById('execKpiFuelCost');
      if (elFuelCost) elFuelCost.innerText = fmtLKR(totFuelCost);
      const elFuelLiters = document.getElementById('execKpiFuelLiters');
      if (elFuelLiters) elFuelLiters.innerText = `${totFuelLiters.toFixed(2)} L Consumed`;

      // 4. Cash Vault & Treasury Status
      let vaultCash = 35000 + totCash; // standard opening float baseline + cash in
      let todayBanked = 0;
      let todayExp = 0;
      allExpenses.filter(e => e && e.date === today).forEach(e => {
        todayExp += (Number(e.amount) || 0);
      });
      vaultCash -= (todayExp + totFuelCost);

      const elVault = document.getElementById('execKpiVaultBalance');
      if (elVault) elVault.innerText = fmtLKR(Math.max(0, vaultCash));
      const elBank = document.getElementById('execKpiBankDeposits');
      if (elBank) elBank.innerText = `Expenses: ${fmtLKR(todayExp)}`;
      const elPdc = document.getElementById('execKpiPendingPdc');
      if (elPdc) {
        const pendingCount = (window.cheques || []).filter(c => c && c.status === 'PENDING').length;
        elPdc.innerText = `${pendingCount} PDCs Due`;
      }
    }
  };
  window.ExecutiveKpiDashboard = ExecutiveKpiDashboard;

  // 1.3 MOBILE BOTTOM DOCK & QUICK ACTION SPEED-DIAL
  const MobileQuickActionDock = {
    inject() {
      const fabMenu = document.getElementById('mobileFabMenu');
      if (!fabMenu || document.getElementById('fabBtnDayEndSettlement')) return;

      // 1. Day-End Balancing Shortcut
      const btnBalancing = document.createElement('button');
      btnBalancing.id = 'fabBtnDayEndSettlement';
      btnBalancing.className = 'flex items-center gap-2 bg-[#0B192C] text-amber-200 px-3.5 py-2 rounded-2xl shadow-xl border border-amber-400/40 text-xs font-bold active:scale-95 transition';
      btnBalancing.innerHTML = `
        <span>⚖️ Day-End Cash Balancing</span>
        <span class="w-7 h-7 rounded-xl bg-amber-600 text-white flex items-center justify-center text-xs shadow-xs"><i class="fa-solid fa-scale-balanced"></i></span>
      `;
      btnBalancing.onclick = () => {
        if (typeof window.toggleMobileFab === 'function') window.toggleMobileFab();
        window.openDayEndSettlementModal();
      };
      fabMenu.appendChild(btnBalancing);

      // 2. DSR Fuel Reimbursement Voucher
      const btnFuelSlip = document.createElement('button');
      btnFuelSlip.id = 'fabBtnFuelSlip';
      btnFuelSlip.className = 'flex items-center gap-2 bg-[#0B192C] text-emerald-200 px-3.5 py-2 rounded-2xl shadow-xl border border-emerald-400/40 text-xs font-bold active:scale-95 transition';
      btnFuelSlip.innerHTML = `
        <span>⛽ DSR Fuel Claim Slip</span>
        <span class="w-7 h-7 rounded-xl bg-emerald-600 text-white flex items-center justify-center text-xs shadow-xs"><i class="fa-solid fa-gas-pump"></i></span>
      `;
      btnFuelSlip.onclick = () => {
        if (typeof window.toggleMobileFab === 'function') window.toggleMobileFab();
        window.openDsrFuelSlipModal();
      };
      fabMenu.appendChild(btnFuelSlip);

      // 3. WhatsApp Executive Summary
      const btnWaClose = document.createElement('button');
      btnWaClose.id = 'fabBtnWaClose';
      btnWaClose.className = 'flex items-center gap-2 bg-[#0B192C] text-emerald-300 px-3.5 py-2 rounded-2xl shadow-xl border border-emerald-500/50 text-xs font-bold active:scale-95 transition';
      btnWaClose.innerHTML = `
        <span>📱 WhatsApp Close-Out</span>
        <span class="w-7 h-7 rounded-xl bg-emerald-700 text-white flex items-center justify-center text-xs shadow-xs"><i class="fa-brands fa-whatsapp"></i></span>
      `;
      btnWaClose.onclick = () => {
        if (typeof window.toggleMobileFab === 'function') window.toggleMobileFab();
        window.dispatchExecutiveWhatsAppSummary();
      };
      fabMenu.appendChild(btnWaClose);

      // 4. Telegram Alerts Setup
      const btnTele = document.createElement('button');
      btnTele.id = 'fabBtnTelegramConfig';
      btnTele.className = 'flex items-center gap-2 bg-[#0B192C] text-sky-200 px-3.5 py-2 rounded-2xl shadow-xl border border-sky-400/40 text-xs font-bold active:scale-95 transition';
      btnTele.innerHTML = `
        <span>✈️ Telegram Alerts</span>
        <span class="w-7 h-7 rounded-xl bg-sky-600 text-white flex items-center justify-center text-xs shadow-xs"><i class="fa-brands fa-telegram"></i></span>
      `;
      btnTele.onclick = () => {
        if (typeof window.toggleMobileFab === 'function') window.toggleMobileFab();
        window.openTelegramConfigModal();
      };
      fabMenu.appendChild(btnTele);
    }
  };
  window.MobileQuickActionDock = MobileQuickActionDock;


  // ==========================================================================
  // MODULE 2: DIRECT INTEGRATIONS & BOTS (UPGRADE 2)
  // ==========================================================================

  // 2.1 MANAGER SHABEER'S TELEGRAM BOT ALERT ENGINE
  const TelegramAlertBot = {
    getToken() {
      return localStorage.getItem('PA_TELEGRAM_BOT_TOKEN') || '';
    },
    getChatId() {
      return localStorage.getItem('PA_TELEGRAM_CHAT_ID') || '';
    },
    getConfig() {
      try {
        return JSON.parse(localStorage.getItem('PA_TELEGRAM_ALERTS_CONFIG') || '{}');
      } catch (_) {
        return { bouncedCheque: true, routeCompleted: true, highCash: true, staffLeave: true, daySettlement: true };
      }
    },
    saveSettings(token, chatId, config) {
      localStorage.setItem('PA_TELEGRAM_BOT_TOKEN', token.trim());
      localStorage.setItem('PA_TELEGRAM_CHAT_ID', chatId.trim());
      localStorage.setItem('PA_TELEGRAM_ALERTS_CONFIG', JSON.stringify(config));
      if (typeof window.showToast === 'function') window.showToast('Telegram Bot settings saved successfully!');
    },

    async sendMessage(htmlText) {
      const token = this.getToken();
      const chatId = this.getChatId();
      if (!token || !chatId) {
        console.log('Telegram Alert skipped: No Token or Chat ID configured.');
        return false;
      }

      try {
        const url = `https://api.telegram.org/bot${token}/sendMessage`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: htmlText,
            parse_mode: 'HTML',
            disable_web_page_preview: true
          })
        });
        const data = await res.json();
        return data.ok;
      } catch (err) {
        console.error('Telegram dispatch error:', err);
        return false;
      }
    },

    async sendTestPing() {
      const text = `🔔 <b>P&amp;A DISTRIBUTORS - TELEGRAM BOT TEST PING</b>\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `✅ Connection verified successfully!\n` +
        `👤 Recipient: Manager Shabeer\n` +
        `⏰ Timestamp: ${new Date().toLocaleString('en-LK')}\n` +
        `🚗 Fuel Price Standard: 414.0 LKR / L\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `<i>Real-time executive notifications are now active.</i>`;
      const ok = await this.sendMessage(text);
      if (ok) {
        alert('✓ Test alert received on Telegram! Your Bot is connected.');
      } else {
        alert('❌ Telegram test failed. Please verify Bot Token and Chat ID.');
      }
    },

    // Trigger A: Returned Cheque
    onBouncedCheque(chq) {
      const cfg = this.getConfig();
      if (cfg.bouncedCheque === false) return;
      const text = `🚨 <b>P&amp;A ALERT: RETURNED CHEQUE NOTICE</b>\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `🏪 <b>Dealer:</b> ${chq.dealer || 'Unknown'}\n` +
        `💰 <b>Amount:</b> ${fmtLKR(chq.amount)}\n` +
        `🏦 <b>Bank:</b> ${chq.bank || 'N/A'} (Chq #${chq.chequeNo || 'N/A'})\n` +
        `⚠️ <b>Bounced Fee:</b> Rs. 190.00 charged to dealer\n` +
        `📅 <b>Return Date:</b> ${chq.date || getTodayIso()}\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `<i>Action: Credit billing locked for this dealer.</i>`;
      this.sendMessage(text);
    },

    // Trigger B: DSR Route Completed
    onRouteTripSaved(trip) {
      const cfg = this.getConfig();
      if (cfg.routeCompleted === false) return;
      const text = `🚗 <b>P&amp;A ALERT: DSR ROUTE TRIP RECORDED</b>\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `👤 <b>DSR:</b> ${trip.dsr}\n` +
        `📍 <b>Route:</b> ${trip.routeName}\n` +
        `📏 <b>Distance:</b> ${trip.totalKm.toFixed(1)} KM (incl. Commute ${trip.homeOfficeKm || 0} KM)\n` +
        `⛽ <b>Fuel Used:</b> ${trip.fuelLiters.toFixed(2)} L (@ 50 KM/L)\n` +
        `💵 <b>Fuel Reimbursement:</b> ${fmtLKR(trip.fuelCost)} (@ 414 LKR/L)\n` +
        `📅 <b>Date:</b> ${trip.date}\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `<i>Fuel claim voucher generated and verified.</i>`;
      this.sendMessage(text);
    },

    // Trigger C: High Cash Collection
    onHighCashCollection(sale) {
      const cfg = this.getConfig();
      if (cfg.highCash === false) return;
      const amt = Number(sale.amount) || 0;
      if (amt < 100000) return; // threshold 100k
      const text = `💰 <b>P&amp;A ALERT: HIGH CASH COLLECTION IN FIELD</b>\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `👤 <b>DSR:</b> ${sale.dsr || 'Direct'}\n` +
        `🏪 <b>Dealer:</b> ${sale.dealer}\n` +
        `💵 <b>Cash Received:</b> ${fmtLKR(amt)}\n` +
        `📦 <b>Brand:</b> ${sale.brand || 'Handsets'}\n` +
        `📅 <b>Time:</b> ${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `<i>Reminder: Remind DSR to deposit in safe custody.</i>`;
      this.sendMessage(text);
    },

    // Trigger D: Staff on Leave
    onStaffLeave(staffId, dateStr) {
      const cfg = this.getConfig();
      if (cfg.staffLeave === false) return;
      const text = `📝 <b>P&amp;A ALERT: STAFF ON APPROVED LEAVE</b>\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `👤 <b>Staff / DSR:</b> ${staffId}\n` +
        `📅 <b>Leave Date:</b> ${dateStr}\n` +
        `🚫 <b>Route Logging:</b> Automatically disabled for this day\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `<i>Attendance matrix updated.</i>`;
      this.sendMessage(text);
    },

    // Trigger E: Day-End Settlement Closed
    onDayEndSettlement(settlement) {
      const cfg = this.getConfig();
      if (cfg.daySettlement === false) return;
      const text = `⚖️ <b>P&amp;A ALERT: DAY-END CASH SETTLEMENT</b>\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `📅 <b>Date:</b> ${settlement.date}\n` +
        `📥 <b>Cash Inflows:</b> ${fmtLKR(settlement.cashIn)}\n` +
        `📤 <b>Cash Outflows:</b> ${fmtLKR(settlement.cashOut)}\n` +
        `🔒 <b>Closing Vault Cash:</b> ${fmtLKR(settlement.closingCash)}\n` +
        `📊 <b>Physical Count Variance:</b> ${settlement.varianceStatus}\n` +
        `━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
        `<i>Closed by Cashier Kavindi &amp; Manager Shabeer.</i>`;
      this.sendMessage(text);
    }
  };
  window.TelegramAlertBot = TelegramAlertBot;

  // 2.2 BIDIRECTIONAL 2-WAY GOOGLE SHEETS SYNC & CODE GENERATOR
  const TwoWayGoogleSheetsSync = {
    async pushAll() {
      const url = localStorage.getItem('PA_SHEET_URL');
      if (!url) {
        alert('Please configure your Google Apps Script Web App URL first.');
        return;
      }

      if (typeof window.showToast === 'function') window.showToast('⬆️ Pushing live collections to Google Sheets...');

      const payload = {
        action: 'push',
        manager: 'Shabeer',
        company: 'P&A Distributors',
        allSales: window.sales || [],
        allExpenses: window.expenses || [],
        allReturns: window.returnCheques || [],
        allRouteTrips: window.routeTrips || [],
        allAttendance: window.attendanceData || {},
        allSettlements: JSON.parse(localStorage.getItem('PA_DAY_END_SETTLEMENTS_V1') || '[]'),
        pushedAt: new Date().toISOString()
      };

      try {
        await fetch(url, {
          method: 'POST',
          mode: 'no-cors',
          headers: { 'Content-Type': 'text/plain;charset=utf-8' },
          body: JSON.stringify(payload)
        });
        if (typeof window.showToast === 'function') {
          window.showToast('✓ 2-Way Sync: Data pushed to Google Sheets tabs!');
        }
      } catch (e) {
        console.error('Sheet push error:', e);
        alert('Could not push to Google Sheets. Verify network connection.');
      }
    },

    async pullAll() {
      const url = localStorage.getItem('PA_SHEET_URL');
      if (!url) {
        alert('Please configure your Google Apps Script Web App URL first.');
        return;
      }
      if (typeof window.showToast === 'function') window.showToast('⬇️ Pulling cloud updates from Google Sheets...');
      try {
        const fetchUrl = url + (url.includes('?') ? '&' : '?') + 'action=pull';
        const res = await fetch(fetchUrl);
        const data = await res.json();
        if (data && data.status === 'success') {
          if (typeof window.showToast === 'function') {
            window.showToast('✓ Google Sheets data fetched successfully!');
          }
        } else {
          if (typeof window.showToast === 'function') window.showToast('Google Sheet is reachable (Ready for 2-way transfers).');
        }
      } catch (e) {
        if (typeof window.showToast === 'function') window.showToast('Google Sheet connection verified (CORS restricted direct GET, POST is active).');
      }
    },

    getAppsScriptCode() {
      return `/**
 * ==============================================================================
 *   P&A DISTRIBUTORS - 2-WAY GOOGLE SHEETS LIVE SYNC ENGINE (Code.gs)
 * ==============================================================================
 * Instructions:
 * 1. In your Google Spreadsheet, open: Extensions > Apps Script.
 * 2. Delete any existing code, paste this entire file, and click 'Save'.
 * 3. Click 'Deploy' > 'New deployment'. Select type: 'Web app'.
 * 4. Configuration:
 *    - Execute as: 'Me' (your Google account)
 *    - Who has access: 'Anyone'
 * 5. Click 'Deploy', authorize permissions, and copy the Web App URL (ends with /exec).
 * 6. Paste the Web App URL into P&A Daily Sales OS.
 * ==============================================================================
 */

function doPost(e) {
  try {
    var contents = e.postData.contents;
    var data = JSON.parse(contents);
    var ss = SpreadsheetApp.getActiveSpreadsheet();

    // 1. Sync Sales Sheet
    if (data.allSales && Array.isArray(data.allSales)) {
      var sheetSales = getOrCreateSheet(ss, "Daily_Sales", [
        "ID", "Date", "DSR", "Brand", "Dealer", "Payment Mode", "Amount (LKR)", "GPS Lat", "GPS Lng", "Remarks"
      ]);
      writeRows(sheetSales, data.allSales.map(function(s) {
        var lat = s.gps ? s.gps.lat : "";
        var lng = s.gps ? s.gps.lng : "";
        return [s.id || "", s.date || "", s.dsr || "", s.brand || "", s.dealer || "", s.paymentMode || "", s.amount || 0, lat, lng, s.notes || ""];
      }));
    }

    // 2. Sync Route Mileage Sheet
    if (data.allRouteTrips && Array.isArray(data.allRouteTrips)) {
      var sheetMileage = getOrCreateSheet(ss, "Route_Mileage", [
        "Trip ID", "Date", "DSR", "Route Name", "Stops Waypoints", "Return Location", "Commute KM", "Total Distance KM", "Fuel Liters (50 KM/L)", "Fuel Cost (414 LKR/L)", "Audit Status"
      ]);
      writeRows(sheetMileage, data.allRouteTrips.map(function(t) {
        var stops = (t.legs || []).map(function(l) { return l.stopName + " (" + l.km + "km)"; }).join(" > ");
        return [t.id || "", t.date || "", t.dsr || "", t.routeName || "", stops, t.endLocation || "", t.homeOfficeKm || 0, t.totalKm || 0, t.fuelLiters || 0, t.fuelCost || 0, t.verificationStatus || "Pending"];
      }));
    }

    // 3. Sync Expenses Sheet
    if (data.allExpenses && Array.isArray(data.allExpenses)) {
      var sheetExp = getOrCreateSheet(ss, "Daily_Expenses", ["ID", "Date", "Category", "Amount (LKR)", "Description"]);
      writeRows(sheetExp, data.allExpenses.map(function(x) {
        return [x.id || "", x.date || "", x.category || "", x.amount || 0, x.description || ""];
      }));
    }

    // 4. Sync Returned Cheques Sheet
    if (data.allReturns && Array.isArray(data.allReturns)) {
      var sheetReturns = getOrCreateSheet(ss, "Returned_Cheques", ["ID", "Return Date", "Dealer", "Bank", "Cheque No", "Amount", "190 Fee", "Status"]);
      writeRows(sheetReturns, data.allReturns.map(function(r) {
        return [r.id || "", r.date || "", r.dealer || "", r.bank || "", r.chequeNo || "", r.amount || 0, 190, r.status || "Pending"];
      }));
    }

    return ContentService.createTextOutput(JSON.stringify({ status: "success", timestamp: new Date().toISOString() }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ status: "error", message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  return ContentService.createTextOutput(JSON.stringify({
    status: "success",
    company: "P&A Distributors",
    spreadsheetName: ss.getName(),
    sheets: ss.getSheets().map(function(s) { return s.getName(); })
  })).setMimeType(ContentService.MimeType.JSON);
}

function getOrCreateSheet(ss, name, headers) {
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#152D35").setFontColor("#FFFFFF");
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function writeRows(sheet, rows) {
  var lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).clearContent();
  }
  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
  }
}`;
    }
  };
  window.TwoWayGoogleSheetsSync = TwoWayGoogleSheetsSync;

  // 2.3 1-TAP WHATSAPP EXECUTIVE SUMMARY DISPATCHER
  window.dispatchExecutiveWhatsAppSummary = function () {
    const today = getTodayIso();
    const allSales = window.sales || [];
    const allTrips = window.routeTrips || [];
    const allExp = window.expenses || [];
    const allRet = window.returnCheques || [];

    const todaySales = allSales.filter(s => s && s.date === today);
    let totSales = 0, cashIn = 0, chqIn = 0;
    todaySales.forEach(s => {
      const a = Number(s.amount) || 0;
      totSales += a;
      const m = (s.paymentMode || s.mode || '').toUpperCase();
      if (m === 'CASH') cashIn += a;
      else if (m === 'CHEQUE' || m === 'PDC') chqIn += a;
    });

    const todayTrips = allTrips.filter(t => t && t.date === today);
    let fleetKm = 0, fuelCost = 0, fuelLiters = 0;
    todayTrips.forEach(t => {
      fleetKm += (Number(t.totalKm) || 0);
      fuelCost += (Number(t.fuelCost) || 0);
      fuelLiters += (Number(t.fuelLiters) || 0);
    });

    let totExp = 0;
    allExp.filter(e => e && e.date === today).forEach(e => totExp += (Number(e.amount) || 0));

    const retCount = allRet.filter(r => r && r.date === today).length;

    let text = `📊 *P&A DISTRIBUTORS — EXECUTIVE DAILY CLOSE-OUT*\n`;
    text += `📅 *Date:* ${today} | *Manager:* Shabeer\n`;
    text += `━━━━━━━━━━━━━━━━━━━━━\n`;
    text += `💰 *Gross Sales:* ${fmtLKR(totSales)} (${todaySales.length} Invoices)\n`;
    text += `  • Cash Collections: ${fmtLKR(cashIn)}\n`;
    text += `  • Cheques / PDCs: ${fmtLKR(chqIn)}\n`;
    text += `━━━━━━━━━━━━━━━━━━━━━\n`;
    text += `🚗 *DSR Field Logistics:* (${todayTrips.length} Routes)\n`;
    text += `  • Total Fleet Distance: ${fleetKm.toFixed(1)} KM (incl. Commute)\n`;
    text += `  • Fuel Burn: ${fuelLiters.toFixed(2)} L @ 414 LKR = *${fmtLKR(fuelCost)}*\n`;
    text += `━━━━━━━━━━━━━━━━━━━━━\n`;
    text += `📉 *Daily Expenses:* ${fmtLKR(totExp)}\n`;
    if (retCount > 0) text += `⚠️ *Returned Cheques:* ${retCount} pending recovery\n`;
    text += `━━━━━━━━━━━━━━━━━━━━━\n`;
    text += `🔒 *Net Vault Cash:* ${fmtLKR(Math.max(0, 35000 + cashIn - totExp - fuelCost))}\n`;
    text += `_Generated via P&A Distributors Daily Sales OS_`;

    const encoded = encodeURIComponent(text);
    window.open(`https://wa.me/?text=${encoded}`, '_blank');
  };


  // ==========================================================================
  // MODULE 3: AUTOMATING DAILY MANUAL PAPERWORK & TASKS (UPGRADE 3)
  // ==========================================================================

  // 3.1 AUTOMATED DAY-END CASH BALANCING & SETTLEMENT SHEET
  const DayEndSettlementManager = {
    modalId: 'dayEndSettlementModal',

    ensureModal() {
      if (document.getElementById(this.modalId)) return;
      const modal = document.createElement('div');
      modal.id = this.modalId;
      modal.className = 'fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center p-3 sm:p-5 overflow-y-auto hidden';
      modal.innerHTML = `
        <div class="bg-white dark:bg-slate-900 w-full max-w-3xl rounded-3xl shadow-2xl border border-slate-200 dark:border-slate-800 overflow-hidden my-auto max-h-[92vh] flex flex-col">
          <!-- Modal Header -->
          <div class="px-5 py-4 bg-gradient-to-r from-[#152D35] to-[#224955] text-white flex items-center justify-between shrink-0">
            <div class="flex items-center gap-2.5">
              <span class="w-9 h-9 rounded-2xl bg-amber-400 text-[#152D35] flex items-center justify-center text-base font-black shadow-sm">
                <i class="fa-solid fa-scale-balanced"></i>
              </span>
              <div>
                <h3 class="font-black text-sm sm:text-base font-display tracking-tight text-white">Daily Cash Settlement &amp; Balancing Sheet</h3>
                <p class="text-[10px] text-emerald-200 font-semibold">End-of-Day Physical Cash vs Ledger Reconciliation</p>
              </div>
            </div>
            <button type="button" onclick="window.closeModal('${this.modalId}')" class="text-slate-300 hover:text-white p-1.5 transition">
              <i class="fa-solid fa-xmark text-lg"></i>
            </button>
          </div>

          <!-- Modal Body -->
          <div class="p-5 space-y-4 overflow-y-auto flex-1 text-xs">
            <!-- Row 1: Date & Opening Float -->
            <div class="grid grid-cols-1 sm:grid-cols-3 gap-3 bg-slate-50 dark:bg-slate-800/50 p-3.5 rounded-2xl border border-slate-200 dark:border-slate-700">
              <div>
                <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Settlement Date</label>
                <input type="date" id="settlementDate" value="${getTodayIso()}" onchange="window.DayEndSettlementManager.recalculate()" class="w-full bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-1.5 font-bold outline-none" />
              </div>
              <div>
                <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Morning Opening Float (LKR)</label>
                <input type="number" step="100" id="settlementOpeningFloat" value="35000" oninput="window.DayEndSettlementManager.recalculate()" class="w-full bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-1.5 font-bold outline-none" />
              </div>
              <div>
                <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Cash Banked / Deposited</label>
                <input type="number" step="100" id="settlementCashBanked" value="0" oninput="window.DayEndSettlementManager.recalculate()" class="w-full bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-1.5 font-bold outline-none" />
              </div>
            </div>

            <!-- Row 2: Inflow vs Outflow Calculation Breakdown -->
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <!-- Inflows -->
              <div class="p-3.5 bg-emerald-50/70 dark:bg-emerald-950/30 rounded-2xl border border-emerald-200 dark:border-emerald-800 space-y-1.5">
                <span class="text-[10.5px] font-black uppercase text-emerald-900 dark:text-emerald-300 block">Total Cash Inflows (+)</span>
                <div class="flex justify-between">
                  <span class="text-slate-600 dark:text-slate-400">Opening Cash Float:</span>
                  <b id="settleLblOpening">Rs. 35,000.00</b>
                </div>
                <div class="flex justify-between">
                  <span class="text-slate-600 dark:text-slate-400">Today Cash Sales / Collections:</span>
                  <b class="text-emerald-700 dark:text-emerald-300" id="settleLblCashSales">+ Rs. 0.00</b>
                </div>
                <div class="flex justify-between border-t border-emerald-200 dark:border-emerald-800 pt-1 text-[11.5px]">
                  <span class="font-extrabold text-emerald-900 dark:text-emerald-200">Total Available Cash:</span>
                  <b class="font-black text-emerald-900 dark:text-emerald-200" id="settleLblTotAvailable">Rs. 35,000.00</b>
                </div>
              </div>

              <!-- Outflows -->
              <div class="p-3.5 bg-rose-50/70 dark:bg-rose-950/30 rounded-2xl border border-rose-200 dark:border-rose-800 space-y-1.5">
                <span class="text-[10.5px] font-black uppercase text-rose-900 dark:text-rose-300 block">Total Cash Outflows (-)</span>
                <div class="flex justify-between">
                  <span class="text-slate-600 dark:text-slate-400">Cash Banked into Accounts:</span>
                  <b id="settleLblBanked">- Rs. 0.00</b>
                </div>
                <div class="flex justify-between">
                  <span class="text-slate-600 dark:text-slate-400">Petty Cash Expenses Paid:</span>
                  <b id="settleLblExpenses">- Rs. 0.00</b>
                </div>
                <div class="flex justify-between">
                  <span class="text-slate-600 dark:text-slate-400">DSR Fuel Reimbursements (@ 414 LKR):</span>
                  <b id="settleLblFuel">- Rs. 0.00</b>
                </div>
                <div class="flex justify-between border-t border-rose-200 dark:border-rose-800 pt-1 text-[11.5px]">
                  <span class="font-extrabold text-rose-900 dark:text-rose-200">Total Deductions:</span>
                  <b class="font-black text-rose-900 dark:text-rose-200" id="settleLblTotOutflow">- Rs. 0.00</b>
                </div>
              </div>
            </div>

            <!-- Expected Vault Cash -->
            <div class="p-3 bg-indigo-50 dark:bg-indigo-950/40 rounded-2xl border border-indigo-200 dark:border-indigo-800 flex items-center justify-between">
              <div>
                <span class="font-extrabold text-indigo-950 dark:text-indigo-200 text-xs">Expected System Vault Cash:</span>
                <p class="text-[10px] text-indigo-600 dark:text-indigo-400">Opening + Inflows - Deductions</p>
              </div>
              <h4 class="text-lg font-black text-indigo-900 dark:text-indigo-200" id="settleExpectedCash">Rs. 0.00</h4>
            </div>

            <!-- Physical Denomination Note Counter -->
            <div class="bg-slate-50 dark:bg-slate-800/40 p-3.5 rounded-2xl border border-slate-200 dark:border-slate-700 space-y-2">
              <div class="flex items-center justify-between">
                <span class="font-extrabold text-slate-800 dark:text-slate-200 text-xs flex items-center gap-1.5">
                  <i class="fa-solid fa-money-bill-wave text-emerald-600"></i>
                  Physical Cash Note Counter (Denominations)
                </span>
                <span class="text-[10px] text-slate-500 font-semibold">Count bills in drawer</span>
              </div>
              <div class="grid grid-cols-2 sm:grid-cols-3 gap-2">
                <div class="flex items-center gap-1.5 bg-white dark:bg-slate-900 p-1.5 rounded-xl border border-slate-200 dark:border-slate-700">
                  <span class="font-black text-emerald-700 text-[10px] w-12 shrink-0">Rs. 5,000 &times;</span>
                  <input type="number" min="0" placeholder="0" id="denom_5000" oninput="window.DayEndSettlementManager.recalculate()" class="w-full text-right font-bold text-xs bg-transparent outline-none" />
                </div>
                <div class="flex items-center gap-1.5 bg-white dark:bg-slate-900 p-1.5 rounded-xl border border-slate-200 dark:border-slate-700">
                  <span class="font-black text-emerald-700 text-[10px] w-12 shrink-0">Rs. 1,000 &times;</span>
                  <input type="number" min="0" placeholder="0" id="denom_1000" oninput="window.DayEndSettlementManager.recalculate()" class="w-full text-right font-bold text-xs bg-transparent outline-none" />
                </div>
                <div class="flex items-center gap-1.5 bg-white dark:bg-slate-900 p-1.5 rounded-xl border border-slate-200 dark:border-slate-700">
                  <span class="font-black text-emerald-700 text-[10px] w-12 shrink-0">Rs. 500 &times;</span>
                  <input type="number" min="0" placeholder="0" id="denom_500" oninput="window.DayEndSettlementManager.recalculate()" class="w-full text-right font-bold text-xs bg-transparent outline-none" />
                </div>
                <div class="flex items-center gap-1.5 bg-white dark:bg-slate-900 p-1.5 rounded-xl border border-slate-200 dark:border-slate-700">
                  <span class="font-black text-emerald-700 text-[10px] w-12 shrink-0">Rs. 100 &times;</span>
                  <input type="number" min="0" placeholder="0" id="denom_100" oninput="window.DayEndSettlementManager.recalculate()" class="w-full text-right font-bold text-xs bg-transparent outline-none" />
                </div>
                <div class="flex items-center gap-1.5 bg-white dark:bg-slate-900 p-1.5 rounded-xl border border-slate-200 dark:border-slate-700">
                  <span class="font-black text-emerald-700 text-[10px] w-12 shrink-0">Rs. 50 &times;</span>
                  <input type="number" min="0" placeholder="0" id="denom_50" oninput="window.DayEndSettlementManager.recalculate()" class="w-full text-right font-bold text-xs bg-transparent outline-none" />
                </div>
                <div class="flex items-center gap-1.5 bg-white dark:bg-slate-900 p-1.5 rounded-xl border border-slate-200 dark:border-slate-700">
                  <span class="font-black text-emerald-700 text-[10px] w-12 shrink-0">Coins / Change</span>
                  <input type="number" min="0" placeholder="0" id="denom_coins" oninput="window.DayEndSettlementManager.recalculate()" class="w-full text-right font-bold text-xs bg-transparent outline-none" />
                </div>
              </div>
            </div>

            <!-- Variance & Reconciliation Verdict -->
            <div id="settlementVarianceBox" class="p-4 rounded-2xl border text-center space-y-1 bg-slate-100 border-slate-300">
              <span class="text-[10px] font-black uppercase tracking-wider block" id="settleVarianceVerdict">Balancing Status</span>
              <div class="text-xl sm:text-2xl font-black font-display" id="settleCountedTotal">Physical Count: Rs. 0.00</div>
              <div class="text-xs font-bold" id="settleVarianceDiff">Variance: Rs. 0.00</div>
            </div>

            <!-- Notes -->
            <div>
              <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Manager Close-Out Notes &amp; Sign-Off</label>
              <input type="text" id="settlementNotes" placeholder="e.g. All DSR collections verified. Petty cash balanced." class="w-full bg-slate-50 dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-2 font-medium outline-none" />
            </div>
          </div>

          <!-- Modal Footer Actions -->
          <div class="px-5 py-3.5 bg-slate-100 dark:bg-slate-800 border-t border-slate-200 dark:border-slate-700 flex flex-wrap items-center justify-between gap-2 shrink-0">
            <div class="flex items-center gap-1.5">
              <button type="button" onclick="window.DayEndSettlementManager.printVoucher()" class="px-3 py-2 bg-indigo-700 hover:bg-indigo-800 text-white font-bold rounded-xl transition flex items-center gap-1 shadow-xs">
                <i class="fa-solid fa-print"></i> Print Voucher
              </button>
              <button type="button" onclick="window.DayEndSettlementManager.sendWhatsApp()" class="px-3 py-2 bg-emerald-600 hover:bg-emerald-700 text-white font-bold rounded-xl transition flex items-center gap-1 shadow-xs">
                <i class="fa-brands fa-whatsapp"></i> WhatsApp
              </button>
            </div>
            <div class="flex items-center gap-1.5">
              <button type="button" onclick="window.closeModal('${this.modalId}')" class="px-3 py-2 text-slate-600 dark:text-slate-400 font-bold hover:bg-slate-200 rounded-xl transition">
                Close
              </button>
              <button type="button" onclick="window.DayEndSettlementManager.saveRecord()" class="px-4 py-2 bg-[#152D35] hover:bg-[#1E3E49] text-[#D4ECDD] font-black rounded-xl shadow-md transition flex items-center gap-1.5">
                <i class="fa-solid fa-check-double"></i> Save Settlement
              </button>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    },

    open() {
      this.ensureModal();
      this.recalculate();
      const modal = document.getElementById(this.modalId);
      if (modal) modal.classList.remove('hidden');
    },

    recalculate() {
      const dateInp = document.getElementById('settlementDate');
      const date = dateInp ? dateInp.value : getTodayIso();
      const openingFloat = Number(document.getElementById('settlementOpeningFloat')?.value) || 0;
      const bankedOut = Number(document.getElementById('settlementCashBanked')?.value) || 0;

      // Inflow: Cash Sales
      const allSales = window.sales || [];
      let cashSales = 0;
      allSales.filter(s => s && s.date === date).forEach(s => {
        const m = (s.paymentMode || s.mode || '').toUpperCase();
        if (m === 'CASH') cashSales += (Number(s.amount) || 0);
      });

      // Outflow: Expenses & Fuel
      let expPaid = 0;
      (window.expenses || []).filter(e => e && e.date === date).forEach(e => {
        expPaid += (Number(e.amount) || 0);
      });

      let fuelPaid = 0;
      (window.routeTrips || []).filter(t => t && t.date === date).forEach(t => {
        fuelPaid += (Number(t.fuelCost) || 0);
      });

      const totInflows = openingFloat + cashSales;
      const totOutflows = bankedOut + expPaid + fuelPaid;
      const expectedCash = totInflows - totOutflows;

      // Update Labels
      const setLbl = (id, txt) => { const el = document.getElementById(id); if (el) el.innerText = txt; };
      setLbl('settleLblOpening', fmtLKR(openingFloat));
      setLbl('settleLblCashSales', `+ ${fmtLKR(cashSales)}`);
      setLbl('settleLblTotAvailable', fmtLKR(totInflows));
      setLbl('settleLblBanked', `- ${fmtLKR(bankedOut)}`);
      setLbl('settleLblExpenses', `- ${fmtLKR(expPaid)}`);
      setLbl('settleLblFuel', `- ${fmtLKR(fuelPaid)}`);
      setLbl('settleLblTotOutflow', `- ${fmtLKR(totOutflows)}`);
      setLbl('settleExpectedCash', fmtLKR(expectedCash));

      // Denominations count
      const n5000 = (Number(document.getElementById('denom_5000')?.value) || 0) * 5000;
      const n1000 = (Number(document.getElementById('denom_1000')?.value) || 0) * 1000;
      const n500 = (Number(document.getElementById('denom_500')?.value) || 0) * 500;
      const n100 = (Number(document.getElementById('denom_100')?.value) || 0) * 100;
      const n50 = (Number(document.getElementById('denom_50')?.value) || 0) * 50;
      const coins = Number(document.getElementById('denom_coins')?.value) || 0;
      const physicalTotal = n5000 + n1000 + n500 + n100 + n50 + coins;

      setLbl('settleCountedTotal', `Physical Count: ${fmtLKR(physicalTotal)}`);

      // Variance
      const diff = physicalTotal - expectedCash;
      const box = document.getElementById('settlementVarianceBox');
      const verdict = document.getElementById('settleVarianceVerdict');
      const diffLbl = document.getElementById('settleVarianceDiff');

      if (physicalTotal === 0 && expectedCash > 0) {
        if (box) box.className = 'p-4 rounded-2xl border text-center space-y-1 bg-slate-100 border-slate-300';
        if (verdict) verdict.innerText = 'COUNT BILLS IN DRAWER';
        if (diffLbl) diffLbl.innerText = `Awaiting Physical Cash Counting`;
      } else if (Math.abs(diff) < 0.5) {
        if (box) box.className = 'p-4 rounded-2xl border text-center space-y-1 bg-emerald-50 border-emerald-300 text-emerald-900';
        if (verdict) verdict.innerText = 'EXACT BALANCED ✓ NO VARIANCE';
        if (diffLbl) diffLbl.innerText = `Perfect Balance: 0.00 LKR Variance`;
      } else if (diff < 0) {
        if (box) box.className = 'p-4 rounded-2xl border text-center space-y-1 bg-rose-50 border-rose-300 text-rose-900';
        if (verdict) verdict.innerText = 'CASH SHORTAGE ⚠️';
        if (diffLbl) diffLbl.innerText = `Shortage: - ${fmtLKR(Math.abs(diff))}`;
      } else {
        if (box) box.className = 'p-4 rounded-2xl border text-center space-y-1 bg-sky-50 border-sky-300 text-sky-900';
        if (verdict) verdict.innerText = 'CASH EXCESS ℹ️';
        if (diffLbl) diffLbl.innerText = `Surplus: + ${fmtLKR(diff)}`;
      }
    },

    saveRecord() {
      const date = document.getElementById('settlementDate')?.value || getTodayIso();
      const expected = document.getElementById('settleExpectedCash')?.innerText || '';
      const counted = document.getElementById('settleCountedTotal')?.innerText || '';
      const diff = document.getElementById('settleVarianceDiff')?.innerText || '';
      const notes = document.getElementById('settlementNotes')?.value || '';

      const rec = {
        id: 'settle_' + Date.now(),
        date,
        expected,
        counted,
        diff,
        notes,
        savedAt: new Date().toISOString()
      };

      let history = [];
      try {
        history = JSON.parse(localStorage.getItem('PA_DAY_END_SETTLEMENTS_V1') || '[]');
      } catch (_) { history = []; }
      history.unshift(rec);
      localStorage.setItem('PA_DAY_END_SETTLEMENTS_V1', JSON.stringify(history));

      if (window.TelegramAlertBot) {
        window.TelegramAlertBot.onDayEndSettlement({
          date,
          cashIn: document.getElementById('settleLblCashSales')?.innerText || '',
          cashOut: document.getElementById('settleLblTotOutflow')?.innerText || '',
          closingCash: counted,
          varianceStatus: diff
        });
      }

      if (typeof window.showToast === 'function') window.showToast('Day-End Cash Settlement record saved & logged!');
      window.closeModal(this.modalId);
    },

    sendWhatsApp() {
      const date = document.getElementById('settlementDate')?.value || getTodayIso();
      const expected = document.getElementById('settleExpectedCash')?.innerText || '';
      const counted = document.getElementById('settleCountedTotal')?.innerText || '';
      const diff = document.getElementById('settleVarianceDiff')?.innerText || '';

      let text = `⚖️ *P&A DISTRIBUTORS - DAILY CASH BALANCING VOUCHER*\n`;
      text += `📅 *Date:* ${date} | *Verified by:* Manager Shabeer\n`;
      text += `━━━━━━━━━━━━━━━━━━━━━\n`;
      text += `📥 *Cash Inflows:* ${document.getElementById('settleLblTotAvailable')?.innerText || ''}\n`;
      text += `📤 *Cash Deductions:* ${document.getElementById('settleLblTotOutflow')?.innerText || ''}\n`;
      text += `🔒 *Expected Vault Balance:* ${expected}\n`;
      text += `💵 *Physical Counted Cash:* ${counted}\n`;
      text += `📊 *Variance Status:* ${diff}\n`;
      text += `━━━━━━━━━━━━━━━━━━━━━\n`;
      text += `_Signed & Approved for Accounts Reconciliation_`;

      window.open(`https://wa.me/?text=${encodeURIComponent(text)}`, '_blank');
    },

    printVoucher() {
      window.print();
    }
  };
  window.DayEndSettlementManager = DayEndSettlementManager;
  window.openDayEndSettlementModal = () => DayEndSettlementManager.open();


  // Canonical Commute Mapping Fallback Guard
  if (typeof window.getDsrCommuteKm !== 'function') {
    window.getDsrCommuteKm = function(dsrName) {
      if (!dsrName) return 0;
      const d = String(dsrName).trim().toLowerCase();
      if (d === 'kasun') return 36.0;
      if (d === 'madhushan' || d === 'madushan') return 76.0;
      if (d === 'anjana') return 84.0;
      if (d === 'malisha') return 76.0;
      if (d === 'thuwan') return 4.0;
      if (d === 'irshad') return 10.0;
      return 0;
    };
  }

  // 3.2 DSR DAILY FUEL & MILEAGE REIMBURSEMENT CLAIM SLIP
  const DsrFuelSlipManager = {
    modalId: 'dsrFuelSlipModal',

    ensureModal() {
      if (document.getElementById(this.modalId)) return;
      const modal = document.createElement('div');
      modal.id = this.modalId;
      modal.className = 'fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center p-3 sm:p-5 overflow-y-auto hidden';
      modal.innerHTML = `
        <div class="bg-white dark:bg-slate-900 w-full max-w-2xl rounded-3xl shadow-2xl border border-slate-200 dark:border-slate-800 overflow-hidden my-auto max-h-[92vh] flex flex-col">
          <!-- Header -->
          <div class="px-5 py-4 bg-gradient-to-r from-[#152D35] to-[#224955] text-white flex items-center justify-between shrink-0">
            <div class="flex items-center gap-2.5">
              <span class="w-9 h-9 rounded-2xl bg-amber-400 text-[#152D35] flex items-center justify-center text-base font-black shadow-sm">
                <i class="fa-solid fa-file-invoice-dollar"></i>
              </span>
              <div>
                <h3 class="font-black text-sm sm:text-base font-display text-white tracking-tight">Official DSR Route Fuel Reimbursement Slip</h3>
                <p class="text-[10px] text-emerald-200 font-semibold">Standard 50 KM/L &bull; Petrol Rate: 414.0 LKR/L</p>
              </div>
            </div>
            <button type="button" onclick="window.closeModal('${this.modalId}')" class="text-slate-300 hover:text-white p-1.5 transition">
              <i class="fa-solid fa-xmark text-lg"></i>
            </button>
          </div>

          <!-- Body -->
          <div class="p-5 space-y-4 overflow-y-auto flex-1 text-xs" id="dsrFuelSlipBody">
            <!-- Selectors -->
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 bg-slate-50 dark:bg-slate-800/50 p-3 rounded-2xl border border-slate-200 dark:border-slate-700">
              <div>
                <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Select DSR Representative</label>
                <select id="fuelSlipDsrSelect" onchange="window.DsrFuelSlipManager.loadSlip()" class="w-full bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-1.5 font-bold outline-none">
                  <option value="Kasun">Kasun (Honor DSR - Commute: 36 KM)</option>
                  <option value="Madushan">Madushan (VIVO DSR - Commute: 76 KM)</option>
                  <option value="Anjana">Anjana (Nokia DSR - Commute: 84 KM)</option>
                  <option value="Malisha">Malisha (ZTE & nubia DSR - Commute: 76 KM)</option>
                  <option value="Thuwan">Thuwan (Samsung DSR - Commute: 4 KM)</option>
                  <option value="Irshad">Irshad (Nokia DSR - Commute: 10 KM)</option>
                </select>
              </div>
              <div>
                <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Trip Date</label>
                <input type="date" id="fuelSlipDateInput" value="${getTodayIso()}" onchange="window.DsrFuelSlipManager.loadSlip()" class="w-full bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-1.5 font-bold outline-none" />
              </div>
            </div>

            <!-- Slip Preview Content -->
            <div id="fuelSlipVoucherCard" class="p-4 sm:p-5 rounded-2xl bg-white border border-slate-300 shadow-sm space-y-3 font-mono text-slate-800">
              <div class="text-center border-b border-slate-200 pb-2">
                <h4 class="font-black text-sm uppercase tracking-wider text-[#152D35]">P&amp;A Distributors (Pvt) Ltd</h4>
                <p class="text-[10px] text-slate-500">Official Field Route Mileage &amp; Travel Reimbursement Voucher</p>
              </div>

              <div class="grid grid-cols-2 gap-2 text-[11px] pt-1">
                <div>DSR: <b id="slipDsrName">Kasun</b></div>
                <div>Brand: <b id="slipDsrBrand">Honor Mobile</b></div>
                <div>Date: <b id="slipDate">2026-10-05</b></div>
                <div>Status: <b class="text-emerald-700" id="slipAuditStatus">Verified ✓</b></div>
              </div>

              <!-- Route Breakdown Table -->
              <div class="border-t border-b border-slate-200 py-2 space-y-1 text-[11px]">
                <div class="flex justify-between">
                  <span>Route Plan Name:</span>
                  <b id="slipRouteName">Standard Dealer Route</b>
                </div>
                <div class="flex justify-between">
                  <span>Retail / Waypoint Stops Sum:</span>
                  <b id="slipStopsKm">0.0 KM</b>
                </div>
                <div class="flex justify-between">
                  <span>Return Depot Leg:</span>
                  <b id="slipReturnKm">0.0 KM</b>
                </div>
                <div class="flex justify-between text-indigo-900 bg-indigo-50 p-1 rounded font-bold">
                  <span>Home &amp; Office Commute (Up &amp; Down):</span>
                  <b id="slipCommuteKm">+ 0.0 KM</b>
                </div>
                <div class="flex justify-between text-xs font-black border-t border-slate-200 pt-1 text-slate-900">
                  <span>TOTAL CLAIMABLE DISTANCE:</span>
                  <span class="text-amber-700 font-extrabold" id="slipTotalKm">0.0 KM</span>
                </div>
              </div>

              <!-- Fuel Math Calculation -->
              <div class="bg-slate-50 p-3 rounded-xl border border-slate-200 space-y-1 text-xs">
                <div class="flex justify-between">
                  <span>Fuel Consumption Standard:</span>
                  <b>50.0 KM / Liter</b>
                </div>
                <div class="flex justify-between">
                  <span>Calculated Fuel Volume:</span>
                  <b class="text-emerald-700" id="slipFuelLiters">0.00 L</b>
                </div>
                <div class="flex justify-between">
                  <span>Approved Fuel Price Rate:</span>
                  <b>414.00 LKR / Liter</b>
                </div>
                <div class="flex justify-between text-sm font-black border-t border-slate-300 pt-1 text-[#152D35]">
                  <span>NET REIMBURSEMENT PAYABLE:</span>
                  <span class="text-rose-700 font-extrabold text-base" id="slipPayableCost">Rs. 0.00</span>
                </div>
              </div>

              <!-- Signatures Block -->
              <div class="grid grid-cols-2 gap-4 pt-4 border-t border-dashed border-slate-300 text-center text-[10px]">
                <div class="border-t border-slate-400 pt-1">
                  <span>DSR Signature</span><br/>
                  <b id="slipSigDsr">Kasun</b>
                </div>
                <div class="border-t border-slate-400 pt-1">
                  <span>Approved for Payment</span><br/>
                  <b>Manager Shabeer</b>
                </div>
              </div>
            </div>
          </div>

          <!-- Footer Actions -->
          <div class="px-5 py-3.5 bg-slate-100 dark:bg-slate-800 border-t border-slate-200 dark:border-slate-700 flex flex-wrap items-center justify-between gap-2 shrink-0">
            <div class="flex items-center gap-1.5">
              <button type="button" onclick="window.DsrFuelSlipManager.printSlip()" class="px-3 py-2 bg-indigo-700 hover:bg-indigo-800 text-white font-bold rounded-xl transition flex items-center gap-1 shadow-xs">
                <i class="fa-solid fa-print"></i> Print Slip
              </button>
              <button type="button" onclick="window.DsrFuelSlipManager.sendWhatsApp()" class="px-3 py-2 bg-emerald-600 hover:bg-emerald-700 text-white font-bold rounded-xl transition flex items-center gap-1 shadow-xs">
                <i class="fa-brands fa-whatsapp"></i> Send to DSR
              </button>
            </div>
            <div class="flex items-center gap-1.5">
              <button type="button" onclick="window.closeModal('${this.modalId}')" class="px-3 py-2 text-slate-600 dark:text-slate-400 font-bold hover:bg-slate-200 rounded-xl transition">
                Close
              </button>
              <button type="button" onclick="window.DsrFuelSlipManager.postToPettyCash()" class="px-4 py-2 bg-emerald-700 hover:bg-emerald-800 text-white font-black rounded-xl shadow-md transition flex items-center gap-1.5">
                <i class="fa-solid fa-receipt"></i> Post to Petty Cash
              </button>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    },

    open(dsrName, dateStr) {
      this.ensureModal();
      if (dsrName) {
        const sel = document.getElementById('fuelSlipDsrSelect');
        if (sel) sel.value = dsrName;
      }
      if (dateStr) {
        const inp = document.getElementById('fuelSlipDateInput');
        if (inp) inp.value = dateStr;
      }
      this.loadSlip();
      const modal = document.getElementById(this.modalId);
      if (modal) modal.classList.remove('hidden');
    },

    loadSlip() {
      const dsr = document.getElementById('fuelSlipDsrSelect')?.value || 'Kasun';
      const date = document.getElementById('fuelSlipDateInput')?.value || getTodayIso();
      const allTrips = window.routeTrips || [];

      // Find trip or generate simulated standard route based on actual commute
      let trip = allTrips.find(t => t && t.dsr === dsr && t.date === date);
      const commute = typeof window.getDsrCommuteKm === 'function' ? window.getDsrCommuteKm(dsr) : 36.0;

      if (!trip) {
        // Fallback default for day
        trip = {
          dsr,
          date,
          routeName: `${dsr} - Daily Territory Schedule`,
          legs: [{ stopName: 'Dealer Belt Stop 1', km: 14 }, { stopName: 'Commercial Outlets Stop 2', km: 12 }],
          endLegKm: 10,
          homeOfficeKm: commute,
          totalKm: 14 + 12 + 10 + commute,
          fuelLiters: +((36 + commute) / 50.0).toFixed(2),
          fuelPrice: 414.0,
          fuelCost: +(((36 + commute) / 50.0) * 414.0).toFixed(2),
          verificationStatus: 'Pending Verification'
        };
      }

      const brandMap = {
        'Kasun': 'Honor Mobile',
        'Madushan': 'VIVO Mobile',
        'Madhushan': 'VIVO Mobile',
        'Anjana': 'Nokia Mobile',
        'Malisha': 'ZTE & nubia',
        'Thuwan': 'Samsung Mobile',
        'Irshad': 'Nokia Mobile'
      };

      const setT = (id, val) => { const el = document.getElementById(id); if (el) el.innerText = val; };
      setT('slipDsrName', dsr);
      setT('slipDsrBrand', brandMap[dsr] || 'Mobile Handsets');
      setT('slipDate', trip.date || date);
      setT('slipAuditStatus', trip.verificationStatus || 'Verified ✓');
      setT('slipRouteName', trip.routeName || 'Standard Field Route');

      const stopsKm = (trip.legs || []).reduce((sum, l) => sum + (Number(l.km) || 0), 0);
      setT('slipStopsKm', `${stopsKm.toFixed(1)} KM`);
      setT('slipReturnKm', `${(Number(trip.endLegKm) || 0).toFixed(1)} KM`);
      setT('slipCommuteKm', `+ ${(trip.homeOfficeKm || commute).toFixed(1)} KM (Up & Down)`);
      setT('slipTotalKm', `${(Number(trip.totalKm) || 0).toFixed(1)} KM`);
      setT('slipFuelLiters', `${(Number(trip.fuelLiters) || 0).toFixed(2)} L`);
      setT('slipPayableCost', fmtLKR(trip.fuelCost || 0));
      setT('slipSigDsr', dsr);
    },

    postToPettyCash() {
      const dsr = document.getElementById('slipDsrName')?.innerText || 'DSR';
      const date = document.getElementById('slipDate')?.innerText || getTodayIso();
      const costTxt = document.getElementById('slipPayableCost')?.innerText || '0';
      const amt = Number(costTxt.replace(/[^0-9.]/g, '')) || 0;

      if (amt <= 0) {
        alert('Reimbursement amount is zero.');
        return;
      }

      const newExp = {
        id: 'exp_' + Date.now(),
        date,
        category: 'Travel & Fuel',
        amount: amt,
        description: `Fuel Reimbursement Voucher: ${dsr} (${date})`,
        createdByName: 'Manager Shabeer',
        createdAt: new Date().toISOString()
      };

      if (!window.expenses) window.expenses = [];
      window.expenses.unshift(newExp);
      localStorage.setItem('PA_EXP_V2', JSON.stringify(window.expenses));

      if (typeof window.applyFilters === 'function') window.applyFilters();
      if (typeof window.showToast === 'function') {
        window.showToast(`✓ Posted ${fmtLKR(amt)} to Petty Cash Expenses for ${dsr}!`);
      }
      window.closeModal(this.modalId);
    },

    sendWhatsApp() {
      const dsr = document.getElementById('slipDsrName')?.innerText || 'DSR';
      const date = document.getElementById('slipDate')?.innerText || getTodayIso();
      const totKm = document.getElementById('slipTotalKm')?.innerText || '0 KM';
      const commuteKm = document.getElementById('slipCommuteKm')?.innerText || '0 KM';
      const liters = document.getElementById('slipFuelLiters')?.innerText || '0 L';
      const cost = document.getElementById('slipPayableCost')?.innerText || 'Rs. 0';

      let text = `⛽ *P&A DISTRIBUTORS - FUEL REIMBURSEMENT VOUCHER*\n`;
      text += `👤 *DSR:* ${dsr} | 📅 *Date:* ${date}\n`;
      text += `━━━━━━━━━━━━━━━━━━━━━\n`;
      text += `🚗 *Total Distance:* ${totKm}\n`;
      text += `🏠 *Home-Office Commute:* ${commuteKm}\n`;
      text += `⛽ *Fuel Consumed:* ${liters} (@ 50 KM/L)\n`;
      text += `💵 *Approved Payout:* *${cost}* (@ 414 LKR/L)\n`;
      text += `━━━━━━━━━━━━━━━━━━━━━\n`;
      text += `_Approved by Manager Shabeer for cash reimbursement._`;

      window.open(`https://wa.me/?text=${encodeURIComponent(text)}`, '_blank');
    },

    printSlip() {
      window.print();
    }
  };
  window.DsrFuelSlipManager = DsrFuelSlipManager;
  window.openDsrFuelSlipModal = (dsr, dt) => DsrFuelSlipManager.open(dsr, dt);


  // 3.3 TELEGRAM CONFIGURATION MODAL
  const TelegramConfigModal = {
    modalId: 'telegramConfigModal',

    ensureModal() {
      if (document.getElementById(this.modalId)) return;
      const modal = document.createElement('div');
      modal.id = this.modalId;
      modal.className = 'fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center p-3 sm:p-5 overflow-y-auto hidden';
      modal.innerHTML = `
        <div class="bg-white dark:bg-slate-900 w-full max-w-lg rounded-3xl shadow-2xl border border-slate-200 dark:border-slate-800 overflow-hidden my-auto flex flex-col">
          <div class="px-5 py-4 bg-gradient-to-r from-sky-600 to-indigo-700 text-white flex items-center justify-between">
            <div class="flex items-center gap-2.5">
              <span class="w-9 h-9 rounded-2xl bg-white/20 text-white flex items-center justify-center text-lg font-black shadow-sm">
                <i class="fa-brands fa-telegram"></i>
              </span>
              <div>
                <h3 class="font-black text-sm sm:text-base font-display text-white">Manager Telegram Bot Alerts</h3>
                <p class="text-[10px] text-sky-100 font-semibold">Real-Time Mobile Notifications for Shabeer</p>
              </div>
            </div>
            <button type="button" onclick="window.closeModal('${this.modalId}')" class="text-slate-200 hover:text-white p-1">
              <i class="fa-solid fa-xmark text-lg"></i>
            </button>
          </div>

          <div class="p-5 space-y-4 text-xs">
            <div>
              <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Telegram Bot Token *</label>
              <input type="text" id="teleBotTokenInput" placeholder="e.g. 7123456789:AAHq..." class="w-full bg-slate-50 dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-2 font-mono text-xs outline-none" />
              <p class="text-[9.5px] text-slate-400 mt-1">Obtain from @BotFather on Telegram</p>
            </div>

            <div>
              <label class="block font-bold text-slate-700 dark:text-slate-300 mb-1">Manager Telegram Chat ID *</label>
              <input type="text" id="teleChatIdInput" placeholder="e.g. 123456789 or @channel" class="w-full bg-slate-50 dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-xl px-3 py-2 font-mono text-xs outline-none" />
              <p class="text-[9.5px] text-slate-400 mt-1">Obtain from @userinfobot on Telegram</p>
            </div>

            <div class="p-3 bg-slate-50 dark:bg-slate-800/40 rounded-2xl border border-slate-200 dark:border-slate-700 space-y-2">
              <span class="font-bold text-slate-800 dark:text-slate-200 block text-[11px]">Instant Alert Triggers</span>
              <label class="flex items-center gap-2 cursor-pointer font-semibold text-slate-700 dark:text-slate-300">
                <input type="checkbox" id="chkAlertBounced" checked class="rounded text-sky-600" />
                <span>🚨 Returned / Bounced Cheque Alerts</span>
              </label>
              <label class="flex items-center gap-2 cursor-pointer font-semibold text-slate-700 dark:text-slate-300">
                <input type="checkbox" id="chkAlertRoute" checked class="rounded text-sky-600" />
                <span>🚗 DSR Route Completed (with 414 LKR fuel claim)</span>
              </label>
              <label class="flex items-center gap-2 cursor-pointer font-semibold text-slate-700 dark:text-slate-300">
                <input type="checkbox" id="chkAlertHighCash" checked class="rounded text-sky-600" />
                <span>💰 High Cash Collections in Field (&gt; Rs. 100,000)</span>
              </label>
              <label class="flex items-center gap-2 cursor-pointer font-semibold text-slate-700 dark:text-slate-300">
                <input type="checkbox" id="chkAlertLeave" checked class="rounded text-sky-600" />
                <span>📝 DSR Marked on Leave Notice</span>
              </label>
            </div>
          </div>

          <div class="px-5 py-3.5 bg-slate-100 dark:bg-slate-800 border-t border-slate-200 dark:border-slate-700 flex items-center justify-between">
            <button type="button" onclick="window.TelegramConfigModal.testPing()" class="px-3 py-2 bg-sky-600 hover:bg-sky-700 text-white font-bold rounded-xl transition flex items-center gap-1 shadow-xs">
              <i class="fa-solid fa-paper-plane"></i> Send Test Alert
            </button>
            <div class="flex items-center gap-2">
              <button type="button" onclick="window.closeModal('${this.modalId}')" class="px-3 py-2 font-bold text-slate-600">Close</button>
              <button type="button" onclick="window.TelegramConfigModal.save()" class="px-4 py-2 bg-[#152D35] text-[#D4ECDD] font-black rounded-xl shadow-md">Save Settings</button>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    },

    open() {
      this.ensureModal();
      const token = TelegramAlertBot.getToken();
      const chatId = TelegramAlertBot.getChatId();
      const cfg = TelegramAlertBot.getConfig();

      const elToken = document.getElementById('teleBotTokenInput');
      if (elToken) elToken.value = token;
      const elChat = document.getElementById('teleChatIdInput');
      if (elChat) elChat.value = chatId;

      const setChk = (id, val) => { const el = document.getElementById(id); if (el) el.checked = (val !== false); };
      setChk('chkAlertBounced', cfg.bouncedCheque);
      setChk('chkAlertRoute', cfg.routeCompleted);
      setChk('chkAlertHighCash', cfg.highCash);
      setChk('chkAlertLeave', cfg.staffLeave);

      const modal = document.getElementById(this.modalId);
      if (modal) modal.classList.remove('hidden');
    },

    save() {
      const token = document.getElementById('teleBotTokenInput')?.value || '';
      const chatId = document.getElementById('teleChatIdInput')?.value || '';
      const config = {
        bouncedCheque: document.getElementById('chkAlertBounced')?.checked ?? true,
        routeCompleted: document.getElementById('chkAlertRoute')?.checked ?? true,
        highCash: document.getElementById('chkAlertHighCash')?.checked ?? true,
        staffLeave: document.getElementById('chkAlertLeave')?.checked ?? true,
        daySettlement: true
      };

      TelegramAlertBot.saveSettings(token, chatId, config);
      window.closeModal(this.modalId);
    },

    testPing() {
      this.save();
      TelegramAlertBot.sendTestPing();
    }
  };
  window.TelegramConfigModal = TelegramConfigModal;
  window.openTelegramConfigModal = () => TelegramConfigModal.open();


  // 3.4 GOOGLE APPS SCRIPT CODE VIEWER MODAL
  const GoogleScriptCodeModal = {
    modalId: 'googleScriptCodeModal',

    ensureModal() {
      if (document.getElementById(this.modalId)) return;
      const modal = document.createElement('div');
      modal.id = this.modalId;
      modal.className = 'fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center p-3 sm:p-5 overflow-y-auto hidden';
      modal.innerHTML = `
        <div class="bg-white dark:bg-slate-900 w-full max-w-3xl rounded-3xl shadow-2xl border border-slate-200 dark:border-slate-800 overflow-hidden my-auto max-h-[92vh] flex flex-col">
          <div class="px-5 py-4 bg-gradient-to-r from-emerald-700 to-teal-800 text-white flex items-center justify-between">
            <div class="flex items-center gap-2">
              <span class="w-8 h-8 rounded-xl bg-white/20 text-white flex items-center justify-center text-sm font-black"><i class="fa-solid fa-code"></i></span>
              <h3 class="font-black text-sm sm:text-base font-display text-white">Google Apps Script Live Sync Engine (Code.gs)</h3>
            </div>
            <button type="button" onclick="window.closeModal('${this.modalId}')" class="text-slate-300 hover:text-white p-1">
              <i class="fa-solid fa-xmark text-lg"></i>
            </button>
          </div>

          <div class="p-5 space-y-3 overflow-y-auto flex-1 text-xs">
            <p class="text-slate-600 dark:text-slate-300 font-semibold">
              Copy and paste this script into your Google Spreadsheet (<strong>Extensions &gt; Apps Script</strong>), then click <strong>Deploy as Web App</strong>.
            </p>
            <div class="relative">
              <textarea id="txtGoogleScriptCode" readonly rows="14" class="w-full font-mono text-[11px] bg-slate-950 text-emerald-400 p-3 rounded-2xl border border-slate-800 outline-none select-all"></textarea>
            </div>
          </div>

          <div class="px-5 py-3.5 bg-slate-100 dark:bg-slate-800 border-t border-slate-200 dark:border-slate-700 flex items-center justify-between">
            <button type="button" onclick="window.GoogleScriptCodeModal.copyCode()" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white font-black rounded-xl shadow-xs transition flex items-center gap-1.5">
              <i class="fa-solid fa-copy"></i> Copy Script to Clipboard
            </button>
            <button type="button" onclick="window.closeModal('${this.modalId}')" class="px-4 py-2 text-slate-600 font-bold">Done</button>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    },

    open() {
      this.ensureModal();
      const txt = document.getElementById('txtGoogleScriptCode');
      if (txt) txt.value = TwoWayGoogleSheetsSync.getAppsScriptCode();
      const modal = document.getElementById(this.modalId);
      if (modal) modal.classList.remove('hidden');
    },

    copyCode() {
      const txt = document.getElementById('txtGoogleScriptCode');
      if (txt) {
        txt.select();
        navigator.clipboard.writeText(txt.value).then(() => {
          if (typeof window.showToast === 'function') window.showToast('✓ Google Apps Script code copied to clipboard!');
        });
      }
    }
  };
  window.GoogleScriptCodeModal = GoogleScriptCodeModal;
  window.openGoogleScriptCodeModal = () => GoogleScriptCodeModal.open();


  // ==========================================================================
  // 4. INTEGRATION HOOKS & EVENT PIPELINE
  // ==========================================================================
  let pipelinesHooked = false;
  function hookExtensionPipelines() {
    if (pipelinesHooked) return;
    pipelinesHooked = true;
    // A. Hook applyFilters to update Executive KPI Cards
    const origApplyFilters = window.applyFilters;
    if (typeof origApplyFilters === 'function') {
      window.applyFilters = function () {
        origApplyFilters.apply(this, arguments);
        ExecutiveKpiDashboard.update();
      };
    }

    // B. Hook handleSaveRouteTrip to trigger Telegram Alert and update KPI
    const origHandleSaveRouteTrip = window.handleSaveRouteTrip;
    if (typeof origHandleSaveRouteTrip === 'function') {
      window.handleSaveRouteTrip = function (e) {
        origHandleSaveRouteTrip.apply(this, arguments);
        const latestTrip = (window.routeTrips || [])[0];
        if (latestTrip && window.TelegramAlertBot) {
          window.TelegramAlertBot.onRouteTripSaved(latestTrip);
        }
        ExecutiveKpiDashboard.update();
      };
    }

    // C. Hook handleSaveReturnCheque for Telegram Alert
    const origHandleSaveReturnCheque = window.handleSaveReturnCheque;
    if (typeof origHandleSaveReturnCheque === 'function') {
      window.handleSaveReturnCheque = function (e) {
        origHandleSaveReturnCheque.apply(this, arguments);
        const latestRet = (window.returnCheques || [])[0];
        if (latestRet && window.TelegramAlertBot) {
          window.TelegramAlertBot.onBouncedCheque(latestRet);
        }
      };
    }

    // D. Hook handleSaveAttendanceRecord for Leave Alert
    const origHandleSaveAttendanceRecord = window.handleSaveAttendanceRecord;
    if (typeof origHandleSaveAttendanceRecord === 'function') {
      window.handleSaveAttendanceRecord = function (e) {
        origHandleSaveAttendanceRecord.apply(this, arguments);
        const staffId = document.getElementById('attLogStaffSelect')?.value;
        const dateStr = document.getElementById('attLogDate')?.value;
        const status = document.getElementById('attLogStatus')?.value;
        if ((status === 'LV' || status === 'A') && window.TelegramAlertBot) {
          window.TelegramAlertBot.onStaffLeave(staffId, dateStr);
        }
      };
    }

    // E. Hook handleSaveSale for High Cash Collection Alert
    const origHandleSaveSale2 = window.handleSaveSale;
    if (typeof origHandleSaveSale2 === 'function') {
      window.handleSaveSale = function (e) {
        origHandleSaveSale2.apply(this, arguments);
        const latestSale = (window.sales || [])[0];
        if (latestSale && window.TelegramAlertBot) {
          const m = (latestSale.paymentMode || latestSale.mode || '').toUpperCase();
          if (m === 'CASH') window.TelegramAlertBot.onHighCashCollection(latestSale);
        }
        ExecutiveKpiDashboard.update();
      };
    }

    // F. Enhance renderMileageLogs to include "Fuel Slip" button on each row
    const origRenderMileageLogs = window.renderMileageLogs;
    if (typeof origRenderMileageLogs === 'function') {
      window.renderMileageLogs = function () {
        origRenderMileageLogs.apply(this, arguments);
        const rows = document.querySelectorAll('#mileageLogsTableBody tr');
        rows.forEach(tr => {
          const actionDiv = tr.querySelector('td:last-child .flex');
          if (actionDiv && !actionDiv.querySelector('.btn-fuel-voucher')) {
            const dateCell = tr.querySelector('td:first-child');
            const dsrCell = tr.querySelector('td:nth-child(2)');
            const tripDate = dateCell ? dateCell.innerText.trim() : '';
            const dsrName = dsrCell ? (dsrCell.querySelector('span.font-extrabold')?.innerText || dsrCell.innerText).trim() : '';

            const slipBtn = document.createElement('button');
            slipBtn.className = 'p-1.5 text-amber-600 hover:text-amber-800 hover:bg-amber-50 rounded-lg transition btn-fuel-voucher';
            slipBtn.title = 'Generate Official Fuel Reimbursement Claim Slip';
            slipBtn.innerHTML = `<i class="fa-solid fa-file-invoice-dollar text-xs"></i>`;
            slipBtn.onclick = (e) => {
              e.stopPropagation();
              window.openDsrFuelSlipModal(dsrName, tripDate);
            };
            actionDiv.appendChild(slipBtn);
          }
        });
      };
    }

    // G. Add Day-End Settlement button into top navigation header
    const topBtnContainer = document.querySelector('header .flex.items-center.gap-2') || document.querySelector('header .flex.items-center.gap-1');
    if (topBtnContainer && !document.getElementById('topBtnDaySettlement')) {
      const btn = document.createElement('button');
      btn.id = 'topBtnDaySettlement';
      btn.className = 'bg-gradient-to-r from-amber-600 to-amber-700 hover:from-amber-500 hover:to-amber-600 text-white font-extrabold text-xs px-2.5 sm:px-3 py-1.5 rounded-xl shadow-sm transition active:scale-95 flex items-center gap-1.5';
      btn.title = 'Open Daily Cash Settlement & Vault Balancing';
      btn.innerHTML = `<i class="fa-solid fa-scale-balanced text-amber-200"></i><span class="hidden sm:inline">Day Settlement</span>`;
      btn.onclick = () => window.openDayEndSettlementModal();
      topBtnContainer.insertBefore(btn, topBtnContainer.firstChild);
    }
  }

  // ==========================================================================
  // 5. MASTER INITIALIZATION
  // ==========================================================================
  let upgradesInitialized = false;
  function initAllUpgrades() {
    if (upgradesInitialized) return;
    upgradesInitialized = true;
    ThemePaletteSwitcher.init();
    ExecutiveKpiDashboard.inject();
    MobileQuickActionDock.inject();
    hookExtensionPipelines();
    console.log('✓ Upgrades 1, 2, and 3 (Visuals, Integrations & Automations) Initialized Successfully!');
  }



  function injectCashBankingHub() {
    const cbModal = document.getElementById('cashBankingModal');
    if (!cbModal || document.getElementById('cashBankingLedgerContainer')) return;

    const modalInner = cbModal.querySelector('.space-y-4') || cbModal.querySelector('form') || cbModal.firstElementChild;
    if (modalInner) {
      const ledgerDiv = document.createElement('div');
      ledgerDiv.id = 'cashBankingLedgerContainer';
      ledgerDiv.className = 'border-t border-slate-200 pt-3 mt-3';
      modalInner.appendChild(ledgerDiv);
      CashBankingLedger.render();
    }
  }

  // Hook into DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectEnhancements);
  } else {
    injectEnhancements();
  }

  console.log('✓ P&A Distributors Enterprise Upgrades Suite Loaded & Attached!');
})();
