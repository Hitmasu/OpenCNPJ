(() => {
  'use strict';

  const $form = document.getElementById('search-form');
  const $input = document.getElementById('cnpj');
  const $btn = document.getElementById('search-btn');
  const $modal = document.getElementById('result-modal');
  const $overlay = $modal ? $modal.querySelector('.modal-overlay') : null;
  const $closeBtns = $modal ? $modal.querySelectorAll('[data-close-modal]') : [];
  const $tabBtnVisual = document.getElementById('tab-btn-visual');
  const $tabBtnJson = document.getElementById('tab-btn-json');
  const $tabPanelVisual = document.getElementById('tab-panel-visual');
  const $tabPanelJson = document.getElementById('tab-panel-json');
  const $modalVisual = document.getElementById('modal-visual');
  const $modalJson = document.getElementById('modal-json');
  const $infoTotal = document.getElementById('info-total');
  const $infoUpdated = document.getElementById('info-updated');
  const $zipDownload = document.getElementById('zip-download');
  const $zipSize = document.getElementById('zip-size');
  const $zipUrl = document.getElementById('zip-url');
  const $zipMd5 = document.getElementById('zip-md5');

  let currentDigits = '';

  function onlyDigits(s) {
    return (s || '').replace(/\D+/g, '');
  }

  function maskCNPJ(digits) {
    const d = (digits || '').slice(0, 14);
    let out = '';
    for (let i = 0; i < d.length; i++) {
      out += d[i];
      if (i === 1 && d.length > 2) out += '.';
      if (i === 4 && d.length > 5) out += '.';
      if (i === 7 && d.length > 8) out += '/';
      if (i === 11 && d.length > 12) out += '-';
    }
    return out;
  }

  function isRepeatedSequence(s) {
    return /^([0-9])\1{13}$/.test(s);
  }

  function validateCNPJ(digits14) {
    const d = onlyDigits(digits14);
    if (d.length !== 14) return false;
    if (isRepeatedSequence(d)) return false;

    const calcDV = (baseDigits, weights) => {
      const sum = baseDigits.split('').reduce((acc, ch, i) => acc + parseInt(ch, 10) * weights[i], 0);
      const r = sum % 11;
      return (r < 2) ? 0 : 11 - r;
    };

    const w1 = [5,4,3,2,9,8,7,6,5,4,3,2];
    const w2 = [6,5,4,3,2,9,8,7,6,5,4,3,2];
    const dv1 = calcDV(d.slice(0, 12), w1);
    const dv2 = calcDV(d.slice(0, 12) + String(dv1), w2);
    return d.endsWith(String(dv1) + String(dv2));
  }

  function pretty(x) {
    try {
      if (typeof x === 'string') {
        try { return JSON.stringify(JSON.parse(x), null, 2); } catch { return x; }
      }
      return JSON.stringify(x, null, 2);
    } catch { return String(x); }
  }

  function formatBytes(bytes){
    const n = Number(bytes || 0);
    if (!isFinite(n) || n <= 0) return '—';
    const units = ['B','KB','MB','GB','TB'];
    const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
    const value = n / Math.pow(1024, i);
    return `${value.toLocaleString('pt-BR', { maximumFractionDigits: 1 })} ${units[i]}`;
  }

  function clear(el){ if (!el) return; while (el.firstChild) el.removeChild(el.firstChild); }

  function setActiveTab(which){
    if (!$tabBtnVisual || !$tabBtnJson || !$tabPanelVisual || !$tabPanelJson) return;
    const isVisual = which === 'visual';
    $tabBtnVisual.classList.toggle('active', isVisual);
    $tabBtnJson.classList.toggle('active', !isVisual);
    $tabBtnVisual.setAttribute('aria-selected', isVisual ? 'true' : 'false');
    $tabBtnJson.setAttribute('aria-selected', !isVisual ? 'true' : 'false');
    $tabPanelVisual.classList.toggle('active', isVisual);
    $tabPanelJson.classList.toggle('active', !isVisual);
  }

  function renderVisual(obj){
    if (!$modalVisual) return;
    clear($modalVisual);
    if (!obj || typeof obj !== 'object') return;
    const dl = document.createElement('dl');
    dl.className = 'kv-list';
    const add = (label, valueOrNode) => {
      if (valueOrNode == null) return;
      if (typeof valueOrNode === 'string' && valueOrNode.trim() === '') return;
      const dt = document.createElement('dt');
      dt.textContent = label;
      const dd = document.createElement('dd');
      if (valueOrNode instanceof Node) dd.appendChild(valueOrNode);
      else dd.textContent = String(valueOrNode);
      dl.appendChild(dt);
      dl.appendChild(dd);
    };

    const addrParts = [obj.logradouro, obj.numero, obj.complemento].filter(Boolean).join(', ');
    const muniUF = [obj.municipio, obj.uf].filter(Boolean).join(' / ');
    const telefones = Array.isArray(obj.telefones) ? obj.telefones : [];
    const tels = telefones.map(t => `${t.ddd || ''} ${t.numero || ''}`.trim()).filter(Boolean).join(' · ');

    add('CNPJ', obj.cnpj);
    add('Razão social', obj.razao_social);
    add('Nome fantasia', obj.nome_fantasia);
    add('Situação', obj.situacao_cadastral);
    add('Data situação', obj.data_situacao_cadastral);
    add('Matriz/Filial', obj.matriz_filial);
    add('Abertura', obj.data_inicio_atividade);
    add('CNAE principal', obj.cnae_principal);
    if (Array.isArray(obj.cnaes_secundarios) && obj.cnaes_secundarios.length){
      const txt = obj.cnaes_secundarios.map(code => String(code)).join(', ');
      add('CNAEs secundários', txt);
    }
    add('Natureza jurídica', obj.natureza_juridica);
    add('Endereço', [addrParts, muniUF, obj.cep].filter(Boolean).join(' · '));
    add('Email', obj.email);
    add('Telefones', tels);
    add('Capital social', obj.capital_social);
    add('Porte', obj.porte_empresa);

    if (Array.isArray(obj.QSA) && obj.QSA.length){
      const names = obj.QSA.map(s => s?.nome_socio || 'Sócio').join(', ');
      add(`Quadro societário (${obj.QSA_count ?? obj.QSA.length})`, names);
    }

    $modalVisual.appendChild(dl);
  }

  function openModal(obj, raw){
    if (!$modal) return;
    setActiveTab('visual');
    renderVisual(obj);
    if ($modalJson) $modalJson.textContent = pretty(raw);
    $modal.classList.add('open');
    $modal.setAttribute('aria-hidden', 'false');
    document.documentElement.style.overflow = 'hidden';
    document.body.style.overflow = 'hidden';
  }

  function closeModal(){
    if (!$modal) return;
    $modal.classList.remove('open');
    $modal.setAttribute('aria-hidden', 'true');
    document.documentElement.style.overflow = '';
    document.body.style.overflow = '';
  }

  // Tab events
  if ($tabBtnVisual) $tabBtnVisual.addEventListener('click', () => setActiveTab('visual'));
  if ($tabBtnJson) $tabBtnJson.addEventListener('click', () => setActiveTab('json'));
  if ($overlay) $overlay.addEventListener('click', closeModal);
  if ($closeBtns && $closeBtns.length) $closeBtns.forEach(btn => btn.addEventListener('click', closeModal));
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModal(); });

  async function loadInfo(){
    try{
      const res = await fetch('https://api.opencnpj.org/info', { headers: { 'Accept': 'application/json' } });
      const info = await res.json();
      if ($infoTotal && typeof info.total === 'number') {
        $infoTotal.textContent = info.total.toLocaleString('pt-BR');
      }
      if ($infoUpdated && info.last_updated) {
        const dt = new Date(info.last_updated);
        try {
          $infoUpdated.textContent = dt.toLocaleString('pt-BR', { year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' });
        } catch {
          $infoUpdated.textContent = String(info.last_updated);
        }
      }

      // ZIP info
      if (info.zip_url) {
        if ($zipDownload) { $zipDownload.href = info.zip_url; }
        if ($zipUrl) {
          $zipUrl.href = info.zip_url;
          $zipUrl.textContent = info.zip_url;
        }
      }
      if (typeof info.zip_size === 'number' && $zipSize) {
        $zipSize.textContent = `Tamanho: ${formatBytes(info.zip_size)}`;
      }
      if (info.zip_md5checksum && $zipMd5) {
        $zipMd5.textContent = info.zip_md5checksum;
      }
    } catch {}
  }

  async function doFetch(digits) {
    $btn.disabled = true;
    $btn.classList.add('loading');

    const url = `https://api.opencnpj.org/${digits}`;
    const controller = new AbortController();
    const to = setTimeout(() => controller.abort(), 12000);

    try {
      const res = await fetch(url, {
        method: 'GET',
        headers: { 'Accept': 'application/json' },
        signal: controller.signal
      });

      const text = await res.text();
      let obj = null; try { obj = JSON.parse(text); } catch {}
      openModal(obj, text);
    } catch (err) {
      openModal(null, 'Erro na consulta.');
    } finally {
      clearTimeout(to);
      $btn.classList.remove('loading');
      $btn.disabled = false;
    }
  }

  // Events
  $input.addEventListener('input', (e) => {
    const digits = onlyDigits(e.target.value);
    currentDigits = digits.slice(0, 14);
    e.target.value = maskCNPJ(currentDigits);
  });

  $form.addEventListener('submit', (e) => {
    e.preventDefault();
    const raw = $input.value;
    const digits = onlyDigits(raw);
    if (digits.length !== 14 || !validateCNPJ(digits)) return;
    doFetch(digits);
  });
  loadInfo();
})();
