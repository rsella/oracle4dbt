
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.dbname\",\
                \"target.host\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.user\",\
                \"target.pass\",\
                \"target.port\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'start',\
                '{{ target.service }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.username }}',\
                '{{ target.get(\"password\", \"\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.dbname\",\
                \"target.host\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.user\",\
                \"target.pass\",\
                \"target.port\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'end',\
                '{{ target.service }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.username }}',\
                '{{ target.get(\"password\", \"\") }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
            )"
    })
}}

select 3 as id from dual
