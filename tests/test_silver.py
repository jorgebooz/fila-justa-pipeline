"""Testes puros do Passo 6, sem banco e sem dependências externas."""
import importlib.util
import pathlib
import unittest

ARQUIVO = pathlib.Path(__file__).resolve().parents[1] / "include/transform/silver.py"
spec = importlib.util.spec_from_file_location("silver", ARQUIVO)
silver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(silver)

class H3Simulado:
    def latlng_to_cell(self, lat, lon, resolucao):
        return f"h3:{resolucao}:{lat}:{lon}"
    def is_valid_cell(self, codigo):
        return codigo.startswith("h3:9:")

class TestSilver(unittest.TestCase):
    def test_mapa_tem_96_distritos(self):
        servicos, tipos, distritos = silver.ler_seeds()
        self.assertEqual(len(distritos), 96)
        self.assertGreater(len(servicos), 0)
        self.assertEqual(len(tipos), 5)

    def test_categorias_ativas_existentes(self):
        servicos, tipos, _ = silver.ler_seeds()
        self.assertTrue(all(cat in tipos for _, cat, ativo in servicos if ativo))

    def test_h3_resolucao_9(self):
        self.assertEqual(silver.calcular_h3(-23.55, -46.63, H3Simulado()),
                         "h3:9:-23.55:-46.63")

    def test_rejeita_outro_h3(self):
        with self.assertRaises(ValueError):
            silver.calcular_h3(-23.55, -46.63, H3Simulado(), 16)

    def test_configuracao_h3_lida_do_weights(self):
        self.assertEqual(silver.ler_resolucao_h3(), 9)

    def test_rejeita_coordenadas_invalidas(self):
        with self.assertRaises(ValueError):
            silver.calcular_h3(0, 0, H3Simulado())

    def test_ids_de_fonte_ficam_distintos_no_sql(self):
        sql = silver.SQL.read_text(encoding="utf-8")
        self.assertIn("'SP156:'", sql)
        self.assertIn("'APP:'", sql)
        self.assertIn("pg_input_is_valid", sql)
        self.assertIn("area_nao_mapeada", sql)
        self.assertIn("coordenada_ausente", sql)
        self.assertIn("data_futura", sql)

    def test_upsert_nao_atualiza_mesmos_registros(self):
        self.assertIn("IS DISTINCT FROM", silver.SQL_UPSERT)
        self.assertIn("ON CONFLICT (ocorrencia_id)", silver.SQL_UPSERT)

if __name__ == "__main__":
    unittest.main()
