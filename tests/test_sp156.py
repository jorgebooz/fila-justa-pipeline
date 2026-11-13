"""Testes unitários sem rede e sem banco. Rodar: python -m unittest discover -s tests -v"""
import importlib.util
import io
import unittest
from pathlib import Path

CAMINHO = Path(__file__).resolve().parents[1] / "include" / "ingest" / "sp156.py"
spec = importlib.util.spec_from_file_location("sp156", CAMINHO)
sp156 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sp156)


class TestIngestaoSP156(unittest.TestCase):
    def test_selecao_ano_e_ordem(self):
        itens = [
            {"name": "Dados do SP156 - 2° TRI 2026", "url": "https://dados.prefeitura.sp.gov.br/b.csv"},
            {"name": "Dados do SP156 - 3° TRI 2025", "url": "https://dados.prefeitura.sp.gov.br/a.csv"},
            {"name": "Dados do SP156 - 1° TRI 2026", "url": "https://dados.prefeitura.sp.gov.br/c.csv"},
        ]
        resultado = sp156.selecionar_recursos(itens, 2026)
        self.assertEqual(set(resultado), {1, 2})
        self.assertEqual(resultado[1]["fonte_arquivo"], "sp156_2026_T1")

    def test_recurso_duplicado_falha(self):
        recurso = {"name": "Dados do SP156 - 1° TRI 2026", "url": "https://dados.prefeitura.sp.gov.br/a.csv"}
        with self.assertRaisesRegex(ValueError, "Mais de um CSV"):
            sp156.selecionar_recursos([recurso, recurso], 2026)

    def test_projecao_sem_dados_de_endereco(self):
        linha = {k: "texto" for k in sp156.COLUNAS}
        linha.update({"Logradouro": "RUA PRIVADA", "Número": "99", "CEP": "00000-000", "Assunto": "PESSOA"})
        valores = sp156.transformar("sp156_2026_T1", 13, linha, "execucao1")
        self.assertEqual(len(valores), len(sp156.CAMPOS))
        self.assertTrue(all(s not in valores for s in ("RUA PRIVADA", "00000-000", "PESSOA")))
        self.assertEqual(valores[:2], ("sp156_2026_T1", 13))

    def test_csv_usa_colunas_reais(self):
        cab = ",".join('"' + c + '"' for c in sp156.COLUNAS)
        corpo = cab + "\n" + ",".join('"teste"' for _ in sp156.COLUNAS) + "\n"
        valores = list(sp156.ler_csv(io.BytesIO(corpo.encode("utf-8-sig"))))
        self.assertEqual(valores[0][0], 1)
        self.assertEqual(valores[0][1]["Serviço"], "teste")

    def test_csv_latin1_com_acentos(self):
        cab = ",".join('"' + c + '"' for c in sp156.COLUNAS)
        dados = dict.fromkeys(sp156.COLUNAS, "")
        dados["Serviço"] = "Remoção de árvore"
        dados["Tema"] = "Rua e bairro"
        out = io.StringIO(newline="")
        escritor = __import__("csv").DictWriter(out, fieldnames=list(sp156.COLUNAS))
        escritor.writeheader()
        escritor.writerow(dados)
        valores = list(sp156.ler_csv(io.BytesIO(out.getvalue().encode("latin-1"))))
        self.assertEqual(valores[0][1]["Serviço"], "Remoção de árvore")

    def test_csv_utf8_com_acentos(self):
        dados = dict.fromkeys(sp156.COLUNAS, "")
        dados["Serviço"] = "Árvore caída"
        out = io.StringIO(newline="")
        escritor = __import__("csv").DictWriter(out, fieldnames=list(sp156.COLUNAS))
        escritor.writeheader()
        escritor.writerow(dados)
        valores = list(sp156.ler_csv(io.BytesIO(out.getvalue().encode("utf-8-sig"))))
        self.assertEqual(valores[0][1]["Serviço"], "Árvore caída")

    def test_csv_exige_colunas(self):
        with self.assertRaisesRegex(ValueError, "ausentes"):
            list(sp156.ler_csv(io.BytesIO(b"Servi\xc3\xa7o\nX\n")))

    def test_sql_upsert_idempotente(self):
        self.assertIn("ON CONFLICT (fonte_arquivo, numero_linha)", sp156.SQL_UPSERT)
        self.assertIn("IS DISTINCT FROM", sp156.SQL_UPSERT)
        self.assertIn("WHERE true", sp156.SQL_UPSERT)


if __name__ == "__main__":
    unittest.main()
