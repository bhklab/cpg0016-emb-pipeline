include: "workflow/Snakefile"


rule all:
    default_target: True
    input:
        MAE_RDS,
        TABLE_ARCHIVE
