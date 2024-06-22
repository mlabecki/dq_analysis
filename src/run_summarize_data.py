from prepare_data import FreddieMac

fm = FreddieMac()

if __name__ == '__main__':
    fm.summarize_data_pyarrow()
    # fm.summarize_data_pyarrow_collist()
