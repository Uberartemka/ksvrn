import sys
sys.path.insert(0, 'D:/pod/backend')
import main
main.init_catalog_tables()
main.seed_data()
print('DB initialized successfully')
