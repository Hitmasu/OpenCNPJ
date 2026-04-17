#!/usr/bin/env node
// Seed local para rodar o Worker OpenCNPJ sem depender do ETL.
//
// Uso:
//   cd src/Worker
//   node scripts/seed-local.mjs
//   npx wrangler dev
//
// Gera:
//   - assets/files/info.json                         (metadata do release)
//   - R2 local (miniflare) via `wrangler r2 object put --local`:
//       files/shards/releases/local/411.ndjson
//       files/shards/releases/local/411.index.bin
//
// Depois do seed, todas as URLs locais abaixo funcionam:
//   http://127.0.0.1:8787/info
//   http://127.0.0.1:8787/41132876000179
//   http://127.0.0.1:8787/schema             (após o time entregar o endpoint)

import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";

const __dirname = dirname(fileURLToPath(import.meta.url));
const WORKER_ROOT = resolve(__dirname, "..");
const ASSETS_DIR = join(WORKER_ROOT, "assets");
const BUCKET = "opencnpj";
const RELEASE_ID = "local";
const SHARD_PREFIX_LENGTH = 3;
const INDEX_HEADER_SIZE = 8;
const INDEX_ENTRY_SIZE = 26;
const CNPJ_LENGTH = 14;
const INDEX_MAGIC = [0x4f, 0x43, 0x49, 0x31]; // "OCI1"

const SEED_RECORDS = [
  {
    cnpj: "41132876000179",
    razao_social: "GALPOES MARTINS LTDA",
    nome_fantasia: "GALPOES MARTINS",
    situacao_cadastral: "Baixada",
    data_situacao_cadastral: "2024-05-09",
    matriz_filial: "Matriz",
    data_inicio_atividade: "2021-03-08",
    cnae_principal: "2330301",
    cnaes_secundarios: ["2330302", "2330303"],
    natureza_juridica: "Sociedade Empresária Limitada",
    tipo_logradouro: "RUA",
    logradouro: "MARIA ANDRADE DO NASCIMENTO",
    numero: "SN",
    complemento: "LOTE 6 LOTE 7 LOTE 12 LOTE 13 LOTE 14",
    bairro: "UNIVERSITARIO",
    cep: "58429340",
    uf: "PB",
    municipio: "CAMPINA GRANDE",
    email: "GALPOES.MARTINS@GMAIL.COM",
    telefones: [{ ddd: "83", numero: "86185304", is_fax: false }],
    capital_social: "29500,00",
    porte_empresa: "Microempresa (ME)",
    opcao_simples: "N",
    data_opcao_simples: "2021-03-08",
    opcao_mei: "N",
    data_opcao_mei: "2021-03-08",
    QSA: [
      {
        nome_socio: "ANDERSON MARTINS",
        cnpj_cpf_socio: "***339234**",
        qualificacao_socio: "Sócio-Administrador",
        data_entrada_sociedade: "2022-12-29",
        identificador_socio: "Pessoa Física",
        faixa_etaria: "21 a 30 anos",
      },
    ],
    cno: null,
  },
  {
    cnpj: "41111111000100",
    razao_social: "EMPRESA ATIVA DE TESTE LTDA",
    nome_fantasia: "ATIVA TESTE",
    situacao_cadastral: "Ativa",
    data_situacao_cadastral: "2019-06-15",
    matriz_filial: "Matriz",
    data_inicio_atividade: "2019-06-15",
    cnae_principal: "6201500",
    cnaes_secundarios: [],
    natureza_juridica: "Empresário (Individual)",
    tipo_logradouro: "AV",
    logradouro: "PAULISTA",
    numero: "1000",
    complemento: "ANDAR 10",
    bairro: "BELA VISTA",
    cep: "01310100",
    uf: "SP",
    municipio: "SAO PAULO",
    email: "contato@ativateste.com",
    telefones: [
      { ddd: "11", numero: "30001234", is_fax: false },
      { ddd: "11", numero: "99991234", is_fax: false },
    ],
    capital_social: "100000,00",
    porte_empresa: "Empresa de Pequeno Porte (EPP)",
    opcao_simples: "S",
    data_opcao_simples: "2019-06-15",
    opcao_mei: "N",
    data_opcao_mei: "",
    QSA: [],
    cno: null,
  },
];

function groupByShardPrefix(records) {
  const groups = new Map();
  for (const record of records) {
    const prefix = record.cnpj.slice(0, SHARD_PREFIX_LENGTH);
    const list = groups.get(prefix) ?? [];
    list.push(record);
    groups.set(prefix, list);
  }
  for (const list of groups.values()) {
    list.sort((a, b) => a.cnpj.localeCompare(b.cnpj));
  }
  return groups;
}

function buildNdjson(records) {
  const encoder = new TextEncoder();
  const entries = [];
  const chunks = [];
  let offset = 0;
  for (const record of records) {
    const line = `${JSON.stringify(record)}\n`;
    const bytes = encoder.encode(line);
    entries.push({ cnpj: record.cnpj, offset, length: bytes.length });
    chunks.push(bytes);
    offset += bytes.length;
  }
  const ndjson = new Uint8Array(offset);
  let cursor = 0;
  for (const chunk of chunks) {
    ndjson.set(chunk, cursor);
    cursor += chunk.length;
  }
  return { ndjson, entries };
}

function buildBinaryIndex(entries) {
  const index = new Uint8Array(INDEX_HEADER_SIZE + entries.length * INDEX_ENTRY_SIZE);
  index.set(INDEX_MAGIC, 0);
  const view = new DataView(index.buffer);
  view.setUint32(4, entries.length, true);

  const encoder = new TextEncoder();
  entries.forEach((entry, i) => {
    const start = INDEX_HEADER_SIZE + i * INDEX_ENTRY_SIZE;
    index.set(encoder.encode(entry.cnpj), start);
    const offsetLow = entry.offset >>> 0;
    const offsetHigh = Math.floor(entry.offset / 0x1_0000_0000);
    view.setUint32(start + CNPJ_LENGTH, offsetLow, true);
    view.setUint32(start + CNPJ_LENGTH + 4, offsetHigh, true);
    view.setUint32(start + CNPJ_LENGTH + 8, entry.length, true);
  });

  return index;
}

function writeAsset(relativePath, data) {
  const dest = join(ASSETS_DIR, relativePath);
  mkdirSync(dirname(dest), { recursive: true });
  writeFileSync(dest, data);
  return dest;
}

function putInR2(key, data) {
  const tmpFile = join(tmpdir(), `opencnpj-seed-${Date.now()}-${Math.random().toString(16).slice(2)}.bin`);
  writeFileSync(tmpFile, data);
  try {
    execFileSync(
      "npx",
      ["wrangler", "r2", "object", "put", `${BUCKET}/${key}`, "--local", "--file", tmpFile],
      { cwd: WORKER_ROOT, stdio: "inherit" },
    );
  } finally {
    try {
      execFileSync("rm", ["-f", tmpFile]);
    } catch {}
  }
}

function main() {
  const groups = groupByShardPrefix(SEED_RECORDS);
  const shardReleases = {};

  const infoJson = {
    storage_release_id: RELEASE_ID,
    default_shard_release_id: RELEASE_ID,
    shard_releases: shardReleases,
    module_shards: {},
  };

  for (const [prefix, records] of groups) {
    shardReleases[prefix] = RELEASE_ID;
    const { ndjson, entries } = buildNdjson(records);
    const index = buildBinaryIndex(entries);

    const ndjsonKey = `files/shards/releases/${RELEASE_ID}/${prefix}.ndjson`;
    const indexKey = `files/shards/releases/${RELEASE_ID}/${prefix}.index.bin`;

    console.log(`[seed] shard ${prefix}: ${records.length} record(s), ${ndjson.length} bytes`);
    putInR2(ndjsonKey, Buffer.from(ndjson));
    putInR2(indexKey, Buffer.from(index));
  }

  const infoPath = writeAsset("files/info.json", `${JSON.stringify(infoJson, null, 2)}\n`);
  console.log(`[seed] info.json escrito em ${infoPath}`);
  putInR2("files/info.json", Buffer.from(`${JSON.stringify(infoJson)}\n`, "utf-8"));

  console.log("");
  console.log("Seed completo. Para rodar:");
  console.log("  npx wrangler dev");
  console.log("");
  console.log("Endpoints locais:");
  console.log("  http://127.0.0.1:8787/info");
  for (const record of SEED_RECORDS) {
    console.log(`  http://127.0.0.1:8787/${record.cnpj}`);
  }
  console.log("  http://127.0.0.1:8787/schema   (disponível após merge do endpoint)");
}

main();
