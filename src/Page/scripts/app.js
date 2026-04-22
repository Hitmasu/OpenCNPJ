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
  const $datasetsStatus = document.getElementById('datasets-status');
  const $datasetsBody = document.getElementById('datasets-body');

  let currentDigits = '';

  const BASE_LENGTH = 12;
  const REGEX_BASE_CNPJ = /^[A-Z\d]{12}$/;
  const REGEX_FULL_CNPJ = /^[A-Z\d]{12}\d{2}$/;
  const REGEX_MASK_CHARACTERS = /[./-]/g;
  const REGEX_INVALID_CHARACTERS = /[^A-Z\d./-]/i;
  const ASCII_ZERO = '0'.charCodeAt(0);
  const CHECK_DIGIT_WEIGHT = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
  const ZEROED_CNPJ = '00000000000000';
  const DATASET_ORDER = ['receita', 'cno', 'rntrc'];
  const DATASET_DETAILS = {
    receita: {
      name: 'Receita Federal',
      description: 'Cadastro base de empresas: estabelecimento, razão social, CNAE, endereço, situação cadastral, Simples/MEI e quadro societário.',
      frequency: 'Mensal',
    },
    cno: {
      name: 'Cadastro Nacional de Obras',
      description: 'Obras vinculadas ao CNPJ responsável, incluindo dados de obra, situação, localização, área e vínculos conhecidos.',
      frequency: 'Diária',
    },
    rntrc: {
      name: 'Registro Nacional de Transportadores Rodoviários de Cargas',
      description: 'Dados do transportador no RNTRC, incluindo número do registro, categoria, situação, município, UF e datas cadastrais.',
      frequency: 'Mensal',
    },
  };

  function removeMask(cnpj) {
    return (cnpj || '').replace(REGEX_MASK_CHARACTERS, '').toUpperCase();
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
    return /^([A-Z\d])\1{13}$/i.test(s);
  }

  function calculateCheckDigits(baseCNPJ) {
    if (REGEX_INVALID_CHARACTERS.test(baseCNPJ)) {
      throw new Error('CNPJ contains invalid characters');
    }

    const raw = removeMask(baseCNPJ);

    if (!REGEX_BASE_CNPJ.test(raw) || raw === ZEROED_CNPJ.slice(0, BASE_LENGTH)) {
      throw new Error('Invalid base CNPJ for check digits calculation');
    }

    const digits = Array.from(raw).map((char) => char.charCodeAt(0) - ASCII_ZERO);
    const sum1 = digits.reduce(
      (acc, digit, index) => acc + digit * CHECK_DIGIT_WEIGHT[index + 1],
      0,
    );
    const dv1 = sum1 % 11 < 2 ? 0 : 11 - (sum1 % 11);
    const sum2 =
      digits.reduce(
        (acc, digit, index) => acc + digit * CHECK_DIGIT_WEIGHT[index],
        0,
      ) +
      dv1 * CHECK_DIGIT_WEIGHT[BASE_LENGTH];
    const dv2 = sum2 % 11 < 2 ? 0 : 11 - (sum2 % 11);

    return `${dv1}${dv2}`;
  }

  function validateCNPJ(cnpj) {
    if (REGEX_INVALID_CHARACTERS.test(cnpj)) {
      return false;
    }

    const raw = removeMask(cnpj);

    if (!REGEX_FULL_CNPJ.test(raw) || raw === ZEROED_CNPJ) {
      return false;
    }

    if (isRepeatedSequence(raw)) {
      return false;
    }

    try {
      const base = raw.slice(0, BASE_LENGTH);
      const providedCheckDigits = raw.slice(BASE_LENGTH);
      const calculatedCheckDigits = calculateCheckDigits(base);
      return providedCheckDigits === calculatedCheckDigits;
    } catch {
      return false;
    }
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

  function formatDate(value) {
    if (!value) return '—';
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) return String(value);
    return dt.toLocaleString('pt-BR', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }

  function formatCount(value) {
    return typeof value === 'number' ? value.toLocaleString('pt-BR') : '—';
  }

  function clear(el){ if (!el) return; while (el.firstChild) el.removeChild(el.firstChild); }

  function renderDatasetsStatus(count) {
    if (!$datasetsStatus) return;

    const label = count === 1 ? 'Atualmente temos 1 base publicada.' : `Atualmente temos ${count} bases publicadas.`;
    $datasetsStatus.innerHTML = [
      `<span class="datasets-status-line">${label}</span>`,
      '<span class="datasets-status-line">As bases podem ser baixadas separadamente em arquivos NDJSON.</span>',
      '<span class="datasets-status-line">Caso queira automatizar, consulte <a href="https://api.opencnpj.org/info" target="_blank" rel="noopener">https://api.opencnpj.org/info</a>.</span>',
    ].join('');
  }

  function renderDatasets(info) {
    if (!$datasetsBody) return;

    clear($datasetsBody);
    const datasets = info?.datasets && typeof info.datasets === 'object' ? info.datasets : {};
    const entries = DATASET_ORDER
      .filter((key) => Object.prototype.hasOwnProperty.call(datasets, key))
      .map((key) => [key, datasets[key]]);

    if (entries.length === 0) {
      if ($datasetsStatus) $datasetsStatus.textContent = 'Nenhuma base publicada foi retornada no momento.';
      return;
    }

    for (const [key, dataset] of entries) {
      const row = document.createElement('tr');
      const nameCell = document.createElement('td');
      const name = document.createElement('strong');
      const description = document.createElement('p');
      const updatedCell = document.createElement('td');
      const frequencyCell = document.createElement('td');
      const countCell = document.createElement('td');
      const filterCell = document.createElement('td');
      const filterCode = document.createElement('code');
      const downloadCell = document.createElement('td');

      name.textContent = DATASET_DETAILS[key].name;
      description.className = 'dataset-desc';
      description.textContent = DATASET_DETAILS[key].description;
      nameCell.appendChild(name);
      nameCell.appendChild(description);
      updatedCell.textContent = formatDate(dataset?.updated_at);
      frequencyCell.textContent = DATASET_DETAILS[key].frequency;
      countCell.textContent = formatCount(dataset?.record_count);
      filterCode.textContent = `datasets=${key}`;
      filterCell.appendChild(filterCode);
      downloadCell.className = 'dataset-download-cell';

      if (dataset?.zip_url) {
        const link = document.createElement('a');
        const sizeMeta = document.createElement('p');
        const checksumMeta = document.createElement('p');
        const md5 = dataset?.zip_md5checksum ? String(dataset.zip_md5checksum) : '—';

        link.className = 'btn';
        link.href = dataset.zip_url;
        link.target = '_blank';
        link.rel = 'noopener';
        link.download = '';
        link.setAttribute('aria-label', `Baixar dataset ${DATASET_DETAILS[key].name}`);
        link.innerHTML = '<svg class="icon" width="18" height="18" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" focusable="false"><path d="M12 16l-5-5h3V4h4v7h3l-5 5zm-7 2h14v2H5v-2z"/></svg>Baixar';

        sizeMeta.className = 'dataset-download-meta';
        sizeMeta.textContent = `Tamanho: ${formatBytes(dataset?.zip_size)}`;

        checksumMeta.className = 'dataset-download-meta dataset-download-checksum';
        checksumMeta.textContent = `MD5: ${md5}`;

        downloadCell.appendChild(link);
        downloadCell.appendChild(sizeMeta);
        downloadCell.appendChild(checksumMeta);
      } else {
        downloadCell.textContent = 'Indisponível';
      }

      row.appendChild(nameCell);
      row.appendChild(updatedCell);
      row.appendChild(frequencyCell);
      row.appendChild(countCell);
      row.appendChild(filterCell);
      row.appendChild(downloadCell);
      $datasetsBody.appendChild(row);
    }

    renderDatasetsStatus(entries.length);
  }

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
        $infoUpdated.textContent = formatDate(info.last_updated);
      }

      renderDatasets(info);
    } catch {
      if ($datasetsStatus) $datasetsStatus.textContent = 'Não foi possível carregar as bases publicadas no momento.';
    }
  }

  async function doFetch(digits) {
    $btn.disabled = true;
    $btn.classList.add('loading');

    const url = `https://api.opencnpj.org/${encodeURIComponent(digits)}`;
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
    const raw = removeMask(e.target.value);
    currentDigits = raw.slice(0, 14);
    e.target.value = maskCNPJ(currentDigits);
  });

  $form.addEventListener('submit', (e) => {
    e.preventDefault();
    const raw = $input.value;
    if (!validateCNPJ(raw)) return;
    const digits = removeMask(raw);
    doFetch(digits);
  });
  loadInfo();
})();
