"""Testes isolados do Passo 7; não requer Postgres nem H3 instalado."""
import importlib.util
import pathlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

ARQUIVO = pathlib.Path(__file__).resolve().parents[1] / "include/transform/clusters.py"
spec = importlib.util.spec_from_file_location("clusters", ARQUIVO)
clusters = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clusters)
BASE = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)


def evento(identificador, dias=0, tipo="buraco", celula="89a", horas=0):
    return (identificador, tipo, celula, BASE + timedelta(days=dias, hours=horas))


class TestClusters(unittest.TestCase):
    def test_janela_da_configuracao(self):
        self.assertEqual(clusters.ler_janela(), 7)

    def test_duplicatas_ate_sete_dias(self):
        mapa = clusters.clusterizar_referencia([evento("x"), evento("y", dias=7)])
        self.assertEqual(mapa["x"], mapa["y"])

    def test_nova_ancora_depois_de_sete_dias(self):
        mapa = clusters.clusterizar_referencia([evento("x"), evento("y", dias=8)])
        self.assertNotEqual(mapa["x"], mapa["y"])

    def test_nao_encadeia_lacunas_consecutivas(self):
        mapa = clusters.clusterizar_referencia([evento("x"), evento("y", dias=6), evento("z", dias=12)])
        self.assertEqual(mapa["x"], mapa["y"])
        self.assertNotEqual(mapa["x"], mapa["z"])

    def test_nao_agrupa_tipos_diferentes(self):
        mapa = clusters.clusterizar_referencia([evento("x"), evento("y", tipo="entulho")])
        self.assertNotEqual(mapa["x"], mapa["y"])

    def test_nao_agrupa_celulas_diferentes(self):
        mapa = clusters.clusterizar_referencia([evento("x"), evento("y", celula="89b")])
        self.assertNotEqual(mapa["x"], mapa["y"])

    def test_ordem_de_entrada_nao_muda_grupos(self):
        eventos = [evento("c", dias=9), evento("a"), evento("b", dias=1)]
        self.assertEqual(clusters.clusterizar_referencia(eventos), clusters.clusterizar_referencia(list(reversed(eventos))))

    def test_empate_resolvido_por_id(self):
        eventos = [evento("z"), evento("a"), evento("b", dias=2)]
        mapa = clusters.clusterizar_referencia(eventos)
        self.assertEqual(len(set(mapa.values())), 1)
        self.assertEqual(mapa["a"], clusters.clusterizar_referencia(list(reversed(eventos)))["a"])

    def test_idempotencia_da_funcao(self):
        eventos = [evento("a"), evento("b", horas=1)]
        self.assertEqual(clusters.clusterizar_referencia(eventos), clusters.clusterizar_referencia(eventos))

    def test_sem_fuso_rejeitado(self):
        with self.assertRaises(ValueError):
            clusters.clusterizar_referencia([("x", "buraco", "89a", datetime(2026, 11, 1))])

    def test_ids_duplicados_rejeitados(self):
        with self.assertRaises(ValueError):
            clusters.clusterizar_referencia([evento("x"), evento("x")])

    def test_configuracao_invalida(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = pathlib.Path(pasta) / "weights.yml"
            arquivo.write_text("cluster_janela_dias: 0\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                clusters.ler_janela(arquivo)

    def test_sql_tem_recursao_janela_ancora(self):
        sql = clusters.SQL_FILE.read_text(encoding="utf-8")
        self.assertIn("WITH RECURSIVE", sql)
        self.assertIn("t.ancora_em", sql)
        self.assertIn("tipo_padrao", sql)
        self.assertIn("h3", sql)
        self.assertIn("ON COMMIT DROP", sql)

    def test_sql_idempotente_com_reconciliacao(self):
        self.assertIn("ON CONFLICT (ocorrencia_id)", clusters.SQL_UPSERT)
        self.assertIn("IS DISTINCT FROM", clusters.SQL_UPSERT)
        self.assertIn("NOT EXISTS", clusters.SQL_DELETE_STALE)


if __name__ == "__main__":
    unittest.main()
