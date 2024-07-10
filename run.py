try:
    # for streamlit >= 1.12.1
    from streamlit.web import bootstrap
except ImportError:
    from streamlit import bootstrap

real_script = 'app.py'
bootstrap.run(real_script, f'run.py {real_script}', [], {})
