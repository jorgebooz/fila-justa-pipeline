"""Testes rápidos do Passo 5; executáveis com unittest padrão, sem Docker."""
import importlib.util
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ARQUIVO = Path(__file__).resolve().parents[1] / "include/ingest/relatos_app_sim.py"
SPEC = importlib.util.spec_from_file_location("relatos_app_sim", ARQUIVO)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def pontos_fake():
    # 96 distritos fictícios somente para testes unitários, sem dados reais.
    return [
        (f"3550308{i:02d}", -23.55 - i * 0.001, -46.60 - i * 0.001)
        for i in range(96)
    ]


class TesteRelatosAppSim(unittest.TestCase):
    def setUp(self):
        self.pontos = pontos_fake()

    def test_determinismo_independente_da_ordem(self):
        a, m1 = MOD.gerar_relatos(self.pontos)
        b, m2 = MOD.gerar_relatos(list(reversed(self.pontos)))
        self.assertEqual(a, b)
        self.assertEqual(m1, m2)

    def test_variacao_da_semente(self):
        a, _ = MOD.gerar_relatos(self.pontos, seed=2026)
        b, _ = MOD.gerar_relatos(self.pontos, seed=2027)
        self.assertNotEqual(a, b)

    def test_96_distritos_em_tres_faixas(self):
        faixas = MOD.sortear_faixas([x[0] for x in self.pontos], 2026)
        self.assertEqual(Counter(faixas.values()), Counter({"baixa": 32, "media": 32, "alta": 32}))
        _, resumo = MOD.gerar_relatos(self.pontos)
        self.assertEqual(resumo["areas"], 96)
        self.assertGreater(resumo["alta"], resumo["media"])
        self.assertGreater(resumo["media"], resumo["baixa"])

    def test_duplicatas_mesmo_tipo_ponto_e_janela(self):
        linhas, resumo = MOD.gerar_relatos(self.pontos)
        por_evento = defaultdict(list)
        for r in linhas:
            chave = (r[4], int(r[0].split("-")[-2]))
            por_evento[chave].append(r)
        self.assertEqual(len(por_evento), resumo["grupos"])
        self.assertEqual(sum(len(g) > 1 for g in por_evento.values()), resumo["grupos_com_duplicatas"])
        self.assertGreater(resumo["grupos_com_duplicatas"], 0)
        for relatos in por_evento.values():
            self.assertEqual(len({(r[1], r[2], r[3]) for r in relatos}), 1)
            datas = [datetime.fromisoformat(r[5]) for r in relatos]
            self.assertLessEqual((max(datas) - min(datas)).total_seconds(), 7200)

    def test_ids_e_campos_sem_pii(self):
        linhas, _ = MOD.gerar_relatos(self.pontos)
        self.assertEqual(len(linhas), len({x[0] for x in linhas}))
        self.assertTrue(all(x[1] in MOD.TIPOS for x in linhas))
        self.assertEqual(set(MOD.CAMPOS), {
            "relato_id", "tipo_raw", "latitude_raw", "longitude_raw",
            "area_id_raw", "aberta_em_raw", "status_raw", "_source", "_run_id",
        })
        self.assertNotIn("assunto", MOD.SQL_UPSERT.lower())
        self.assertNotIn("logradouro", MOD.SQL_UPSERT.lower())
        self.assertNotIn("cep", MOD.SQL_UPSERT.lower())

    def test_datas_2026_sem_futuro_em_relacao_a_base(self):
        linhas, _ = MOD.gerar_relatos(self.pontos)
        base = MOD.validar_data_base(MOD.DATA_BASE_PADRAO)
        self.assertTrue(all(datetime.fromisoformat(r[5]) <= base for r in linhas))
        self.assertTrue(all(datetime.fromisoformat(r[5]).year == 2026 for r in linhas))

    def test_data_sem_fuso_e_rejeitada(self):
        with self.assertRaisesRegex(ValueError, "fuso"):
            MOD.validar_data_base("2026-11-11T12:00:00")
        with self.assertRaisesRegex(ValueError, "2026"):
            MOD.validar_data_base("2025-11-11T12:00:00-03:00")

    def test_mapa_e_pontos_arquivos_de_entrada(self):
        with tempfile.TemporaryDirectory() as d:
            mapa = Path(d) / "mapa.csv"
            pts = Path(d) / "pontos.csv"
            mapa.write_text("area_id,municipio_cod\n" + "".join(f"{p[0]},3550308\n" for p in self.pontos), encoding="utf-8")
            pts.write_text("area_id,latitude,longitude\n" + "".join(f"{p[0]},{p[1]},{p[2]}\n" for p in self.pontos), encoding="utf-8")
            self.assertEqual(MOD.carregar_pontos(pts, mapa), self.pontos)
            pts.write_text("area_id,latitude,longitude\n" + "".join(f"{p[0]},{p[1]},{p[2]}\n" for p in self.pontos[:-1]), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "96"):
                MOD.carregar_pontos(pts, mapa)

    def test_upsert_atualiza_apenas_se_conteudo_mudar(self):
        self.assertIn("ON CONFLICT (relato_id)", MOD.SQL_UPSERT)
        self.assertIn("IS DISTINCT FROM", MOD.SQL_UPSERT)
        self.assertNotIn("TRUNCATE", MOD.SQL_UPSERT.upper())
        self.assertIn("ON COMMIT DROP", MOD.SQL_STAGE)


if __name__ == "__main__":
    unittest.main()
