from pathlib import Path

for p in Path('D:/workspace/').glob('**/*pattern*.java'):
    if 'keyword' not in p.read_text() and all( filter not in p.name for filter in ['SFTP','Consumer','File','Procuder'] ) :
        print(f"{p.name}:\n")