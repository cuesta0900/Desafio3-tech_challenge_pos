"""
Configuracao compartilhada do pipeline State of Data Brasil (Bronze/Silver/Gold).

Usada pelos 3 Glue Jobs separados por camada: bronze_job.py, silver_job.py e
gold_job.py (ver pipeline/README.md para instrucoes de deploy).

"""

# troque pelo nome do bucket criado no S3
BUCKET = ""

PATHS = {
    "raw": f"{BUCKET}/raw",
    "bronze": f"{BUCKET}/bronze",
    "silver": f"{BUCKET}/silver",
    "gold": f"{BUCKET}/gold",
}

# nomes dos arquivos exatamente como estao no S3 (raw/)
ARQUIVOS = {
    2023: "State_of_data_BR_2023_Kaggle - df_survey_2023.csv",
    2024: "Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv",
    2025: "Final Dataset - State of Data 2025-2026 - Kaggle.csv",
}

# Mapeamento REAL validado por edicao. Campo -> nome da coluna na Bronze.
# 2023 usa codigos "P0"-style pois o header original virou tupla-string;
# ao ler o Parquet da Bronze, o Spark ja usa esse nome de coluna tal como
# veio do CSV -- entao mapeamos por essa string literal.
COLMAP = {
    2023: {
        "genero": "('P1_b ', 'Genero')",
        "uf": "('P1_i_1 ', 'uf onde mora')",
        "regiao": "('P1_i_2 ', 'Regiao onde mora')",
        "cargo": "('P2_f ', 'Cargo Atual')",
        "senioridade": "('P2_g ', 'Nivel')",
        "faixa_salarial": "('P2_h ', 'Faixa salarial')",
        "usa_ia": "('P4_m ', 'Utiliza ChatGPT ou LLMs no trabalho?')",
    },
    2024: {
        "genero": "1.b_genero",
        "uf": "1.i.1_uf_onde_mora",
        "regiao": "1.i.2_regiao_onde_mora",
        "cargo": "2.f_cargo_atual",
        "senioridade": "2.g_nivel",
        "faixa_salarial": "2.h_faixa_salarial",
        "usa_ia": "4.m_usa_chatgpt_ou_copilot_no_trabalho?",
    },
    2025: {
        "genero": "1.b_genero",
        "uf": "1.i.1_uf_onde_mora",
        "regiao": "1.i.2_regiao_onde_mora",
        "cargo": "2.f_cargo_atual",
        "senioridade": "2.g_nivel",
        "faixa_salarial": "2.h_faixa_salarial",
        "usa_ia": "4.j_usa_chatgpt_ou_copilot_no_trabalho?",
    },
}

# Nota sobre P4_m em 2023: o nome exato da coluna no arquivo real pode variar
# ligeiramente em acentuacao/espacos -- valide com
#   [c for c in df.columns if c.startswith("('P4_m")]
# antes de rodar em producao, e ajuste a string acima se necessario.

REGIAO_POR_UF = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}

# Colunas de linguagem sao "dummies" (uma coluna por linguagem, valor 1/0),
# com prefixo e nomes diferentes por edicao. Mapeamos o prefixo de cada ano;
# o sufixo da coluna ja e o proprio nome da linguagem.
LINGUAGEM_PREFIXO = {
    2023: "('P4_d_",   # colunas como "('P4_d_1 ', 'SQL')"
    2024: "4.d.",       # colunas como "4.d.1_SQL"
    2025: "4.c.",       # colunas como "4.c.1_SQL"
}
