const { createApp } = Vue;

createApp({
  delimiters: ['[[', ']]'],
  data() {
    return {
      state: {
        status: 'idle',
        phase: 'waiting',
        message: '等待执行',
        fetched: 0,
        fetch_total: 0,
        analyzed: 0,
        analyze_total: 0,
        last_run: null,
      },
      records: [],
      selectedLimit: 'all',
      timer: null,
    };
  },
  computed: {
    pdfRecords() {
      return this.records.filter((item) => item.pdf_filename);
    },
    analysisRecords() {
      return this.records.filter((item) => item.stage === 'done');
    },
    prettyStatus() {
      const status = this.state.status || 'idle';
      const phase = this.state.phase || 'waiting';
      return `${status} / ${phase}`;
    },
  },
  methods: {
    async safeJsonFetch(url, fallback) {
      try {
        const response = await fetch(url, { cache: 'no-store' });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        return await response.json();
      } catch (error) {
        console.error(`请求失败: ${url}`, error);
        return fallback;
      }
    },
    async fetchState() {
      const data = await this.safeJsonFetch('/api/state', null);
      if (data) {
        this.state = data;
      } else {
        this.state = {
          ...this.state,
          status: 'offline',
          phase: 'disconnected',
          message: '后端未连接',
        };
      }
    },
    async fetchRecords() {
      const data = await this.safeJsonFetch('/api/reports', null);
      if (data) {
        this.records = data;
      }
    },
    async refreshAll() {
      await Promise.all([this.fetchState(), this.fetchRecords()]);
    },
    async startRun(force) {
      try {
        const response = await fetch('/api/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          cache: 'no-store',
          body: JSON.stringify({
            force,
            limit: this.selectedLimit,
          }),
        });
        const data = await response.json();
        window.alert(data.message || '任务已启动');
      } catch (error) {
        console.error('启动任务失败', error);
        window.alert('无法连接后端，请先确认 python caibao.py 正在运行。');
      }
      await this.refreshAll();
    },
    async clearCurrentList() {
      const confirmed = window.confirm('这会清空当前页面列表和 JSON 缓存，是否继续？');
      if (!confirmed) {
        return;
      }
      try {
        const response = await fetch('/api/clear', {
          method: 'POST',
          cache: 'no-store',
        });
        const data = await response.json();
        window.alert(data.message || '已清空');
      } catch (error) {
        console.error('清空列表失败', error);
        window.alert('清空失败，请确认后端正在运行。');
      }
      await this.refreshAll();
    },
    pdfUrl(item) {
      return `/pdfs/${encodeURIComponent(item.pdf_filename || '')}`;
    },
    formatList(value) {
      if (!value || !value.length) {
        return '暂无';
      }
      return value.join('；');
    },
    stageClass(stage) {
      if (stage === 'done') return 'stage-done';
      if (stage === 'analyzing') return 'stage-analyzing';
      if (stage === 'error') return 'stage-error';
      return '';
    },
    viewClass(view) {
      const text = String(view || '');
      if (text.includes('偏多')) return 'view-bullish';
      if (text.includes('承压')) return 'view-bearish';
      return 'view-neutral';
    },
  },
  async mounted() {
    await this.refreshAll();
    this.timer = window.setInterval(() => {
      this.refreshAll();
    }, 3000);
  },
  beforeUnmount() {
    if (this.timer) {
      window.clearInterval(this.timer);
    }
  },
}).mount('#app');
