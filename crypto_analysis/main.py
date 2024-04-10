from crypto_analysis.data.data_extractor import DataExtractor

# Get information genrral information for all cryptos and store it in a pandas df.
data = DataExtractor.request_all_crypto_info(save_file=True)
