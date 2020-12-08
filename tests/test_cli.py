import pytest
import es2json.cli

std_argv = ["-server", "http://localhost:9200"]

def run_cli(argv, ercode=0):
    with pytest.raises(SystemExit) as e:
        es2json.cli.run(argv= std_argv + argv)
    assert e.value.code == ercode
    return e

def test_cli_availability():
    """ Test CLI help function which should always work """
    assert run_cli(["-h"])

def test_cli_headless_ign_source(capsys):
    """ Test unwanted combination of -ign-source and -headless """
    run_cli(["-ign-source", "-headless"], -1)

    out, err = capsys.readouterr()
    assert out == ''
    assert err.strip() == "ERROR! do not use -headless and -ign-source at the same Time!"
    

if __name__ == '__main__':
    pytest.main()
