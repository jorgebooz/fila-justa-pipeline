"""Testes isolados, sem internet ou PostgreSQL.
Execute: python -m unittest discover -s tests -v
"""
import importlib.util
import io
import struct
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def carregar_modulo(nome, arquivo):
    spec = importlib.util.spec_from_file_location(nome, ROOT / arquivo)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


clima = carregar_modulo('clima', 'include/ingest/clima.py')
malha = carregar_modulo('malha', 'include/ingest/gerar_pontos_clima.py')


def exemplo_meteo(n=1):
    return [{
        'latitude': -23.54,
        'longitude': -46.61,
        'hourly': {'time': [1794398400 + 3600 * i for i in range(48)],
                   'precipitation': [float(i % 4) / 10 for i in range(48)]},
        'hourly_units': {'precipitation': 'mm'},
    } for _ in range(n)]


class PrevisaoTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = datetime(2026, 11, 11, 22, tzinfo=timezone.utc)

    def test_48_horas(self):
        rows = clima.interpretar_resposta([('355030801', -23.5, -46.6)],
                                         exemplo_meteo(1)[0], self.snapshot, 'teste')
        self.assertEqual(len(rows), 48)
        self.assertEqual(rows[0][0], '355030801')
        self.assertTrue(rows[1][1].startswith('2026-11-11T'))

    def test_multi_local_ordem_e_mapeamento(self):
        areas = [('355030801', -23.5, -46.6), ('355030802', -23.6, -46.7)]
        rows = clima.interpretar_resposta(areas, exemplo_meteo(2), self.snapshot, 'teste')
        self.assertEqual(len(rows), 96)
        self.assertEqual(rows[47][0], '355030801')
        self.assertEqual(rows[48][0], '355030802')

    def test_incompleto_falha(self):
        payload = exemplo_meteo(1)[0]
        payload['hourly']['time'] = payload['hourly']['time'][:-1]
        with self.assertRaises(ValueError):
            clima.interpretar_resposta([('355030801', -23.5, -46.6)], payload, self.snapshot, 'teste')

    def test_nulo_falha(self):
        payload = exemplo_meteo(1)[0]
        payload['hourly']['precipitation'][0] = None
        with self.assertRaises(ValueError):
            clima.interpretar_resposta([('355030801', -23.5, -46.6)], payload, self.snapshot, 'teste')

    def test_monta_url_de_48_horas(self):
        url = clima.montar_url([('355030801', -23.5, -46.6)])
        self.assertIn('forecast_hours=48', url)
        self.assertIn('timeformat=unixtime', url)

    def test_upsert_idempotente_mesmo_snapshot(self):
        self.assertIn('ON CONFLICT (area_id, hora_raw, consultado_em)', clima.SQL_UPSERT)
        self.assertIn('IS DISTINCT FROM', clima.SQL_UPSERT)


class GeometriaTests(unittest.TestCase):
    def test_centroide_quadrado(self):
        anel = [(-46.65, -23.55), (-46.64, -23.55), (-46.64, -23.54),
                (-46.65, -23.54), (-46.65, -23.55)]
        header = struct.pack('<i', 5) + struct.pack('<4d', -46.65, -23.55, -46.64, -23.54)
        registro = header + struct.pack('<2i', 1, len(anel)) + struct.pack('<i', 0)
        registro += b''.join(struct.pack('<2d', *p) for p in anel)
        lat, lon, metodo = malha.centroide_poligono(registro)
        self.assertAlmostEqual(lat, -23.545, 4)
        self.assertAlmostEqual(lon, -46.645, 4)
        self.assertEqual(metodo, 'centroide')

    def test_extrai_codigo_centroide_de_zip_sintetico(self):
        def campo(nome, largura):
            return nome.encode("ascii").ljust(11, b"\0") + b"C" + b"\0" * 4 + bytes((largura, 0)) + b"\0" * 14

        cab_dbf = (b"\x03\x7e\x0b\x0b" + struct.pack("<IHH", 1, 97, 17)
                   + b"\0" * 20 + campo("CD_MUN", 7) + campo("CD_DIST", 9) + b"\x0d")
        dbf = cab_dbf + b" " + b"3550308" + b"355030801" + b"\x1a"
        anel = [(-46.65, -23.55), (-46.64, -23.55), (-46.64, -23.54),
                (-46.65, -23.54), (-46.65, -23.55)]
        poligono = (struct.pack('<i', 5) + struct.pack('<4d', -46.65, -23.55, -46.64, -23.54)
                    + struct.pack('<2i', 1, len(anel)) + struct.pack('<i', 0)
                    + b''.join(struct.pack('<2d', *p) for p in anel))
        shp_header = bytearray(100)
        struct.pack_into('>i', shp_header, 0, 9994)
        struct.pack_into('<i', shp_header, 28, 1000)
        struct.pack_into('<i', shp_header, 32, 5)
        shp = bytes(shp_header) + struct.pack('>ii', 1, len(poligono)//2) + poligono
        b = io.BytesIO()
        with zipfile.ZipFile(b, 'w') as z:
            z.writestr('sample.shp', shp)
            z.writestr('sample.dbf', dbf)
            z.writestr('sample.prj', 'GEOGCS["GCS_SIRGAS_2000"]')
        coords = malha.extrair_pontos(b.getvalue(), {'355030801'})
        self.assertEqual(set(coords), {'355030801'})
        self.assertAlmostEqual(coords['355030801'][0], -23.545, places=4)

    def test_rejeita_sistema_projetado(self):
        # DBF de 1 distrito e geometria sintética para garantir que CRS
        # projetado não seja usado como latitude/longitude.
        def campo(nome, largura):
            return nome.encode("ascii").ljust(11, b"\0") + b"C" + b"\0" * 4 + bytes((largura, 0)) + b"\0" * 14

        dbf_header = (
            b"\x03\x7e\x0b\x0b" + struct.pack("<IHH", 1, 97, 17) + b"\0" * 20
            + campo("CD_MUN", 7) + campo("CD_DIST", 9) + b"\x0d"
        )
        dbf_data = dbf_header + b" " + b"3550308" + b"355030801" + b"\x1a"
        with io.BytesIO() as b:
            with zipfile.ZipFile(b, 'w') as z:
                z.writestr('sample.shp', b'0' * 100)
                z.writestr('sample.dbf', dbf_data)
                z.writestr('sample.prj', 'PROJCS["UTM"]')
            with self.assertRaisesRegex(ValueError, "não declara SIRGAS"):
                malha.extrair_pontos(b.getvalue(), {'355030801'})


if __name__ == '__main__':
    unittest.main()
