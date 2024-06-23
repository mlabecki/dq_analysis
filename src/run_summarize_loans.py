from prepare_data import FreddieMac

fm = FreddieMac()

if __name__ == '__main__':
    # fm.summarize_loans_pandas()
    fm.summarize_loans_pyarrow()