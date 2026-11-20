import pytest

from catalogos import normalizar_nombre_pais


@pytest.mark.parametrize(
    "original, esperado",
    [
        ("T\u00c3\u00bcrkiye", "Türkiye"),
        ("C\u00c3\u00b4te d'Ivoire", "Côte d'Ivoire"),
        ("Cura\u00c3\u00a7ao", "Curaçao"),
        ("Saint Barth\u00c3\u00a9lemy", "Saint Barthélemy"),
    ],
)
def test_repara_los_cuatro_casos_observados_y_es_idempotente(original, esperado):
    corregido = normalizar_nombre_pais(original)
    assert corregido == esperado
    assert normalizar_nombre_pais(corregido) == esperado


@pytest.mark.parametrize(
    "nombre",
    [None, "", "USA", "Costa Rica", "Türkiye", "Côte d'Ivoire", "Curaçao",
     "Saint Barthélemy", "Åland", "São Tomé", "日本"],
)
def test_conserva_nombres_validos_y_nulos(nombre):
    assert normalizar_nombre_pais(nombre) == nombre


@pytest.mark.parametrize(
    "nombre, mensaje",
    [
        ("T\ufffdrkiye", "carácter de reemplazo"),
        ("Cura\u00c3\u00a7ao y São Tomé", "sin pérdida de caracteres"),
        ("T\u00c3\u0083\u00c2\u00bcrkiye", "sigue mostrando codificación dañada"),
    ],
)
def test_rechaza_texto_que_no_puede_reparar_de_forma_segura(nombre, mensaje):
    with pytest.raises(ValueError, match=mensaje):
        normalizar_nombre_pais(nombre)
