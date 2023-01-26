##############################################################################################
# RFM Analizi ve Müşteri Segmentasyonu
###############################################################################################

##############################################################################################
# İş Problemi
###############################################################################################

# İngiltere merkezli perakende şirketi müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama
# stratejileri belirlemek istemektedir.Ortak davranışlar sergileyen müşteri segmentleri özelinde
# pazarlama çalışmaları yapmanın gelir artışı sağlayacağınıdüşünmektedir.
# Segmentlere ayırmak için RFM analizi kullanılacaktır.

##############################################################################################
# Veri Seti Hikayesi
###############################################################################################

# Online Retail II isimli veri seti İngiltere merkezli bir perakende şirketinin 01/12/2009 - 09/12/2011 tarihleri
# arasındaki online satış işlemlerini içeriyor. Şirketin ürün kataloğunda hediyelik eşyalar yer almaktadır ve çoğu
# müşterisinin toptancı olduğu bilgisi mevcuttur


# Değişkenler

# Invoice: Fatura Numarası ( Eğer bu kod C ile başlıyorsa işlemin iptal edildiğini ifade eder )
# StockCode: Ürün kodu ( Her bir ürün için eşsiz )
# Description: Ürün ismi
# Quantity: Ürün adedi ( Faturalardaki ürünlerden kaçar tane satıldığı)
# InvoiceDate: Fatura tarihi
# UnitPrice: Fatura fiyatı ( Sterlin )
# CustomerID: Eşsiz müşteri numarası
# Country: Ülke ismi


##############################################################################################
# Veriyi Anlama ve Hazırlama
###############################################################################################

import datetime as dt
import pandas as pd
import numpy as np
pd.set_option("display.float_format", lambda x: "%.3f" %x)
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

df_ = pd.read_excel("datasets\online_retail_II.xlsx", sheet_name="Year 2010-2011")
df = df_.copy()

# Betimsel İstatistikler
df.head()
df.describe().T

# Eksik Gözlem Kontrolü

df.isnull().sum()

# Eksik Gözlemleri Veri Setinden Çıkartma

df.dropna(inplace=True)

# Eşsiz Ürün Sayısı

df["Description"].nunique()

# Hangi üründen kaçar adet vardır?

df.groupby("Description")["Quantity"].sum()

# En çok sipariş edilen 5 ürün

df.groupby("Description")["Quantity"].sum().sort_values(ascending=False).head()

# Faturadaki"C" iptal edilen işlemleri göstermektedir. Bunları veri setinden çıkartalım.

df = df[~df["Invoice"].str.contains("C", na=False)]

# Toplam kazancı ifade eden değişken oluşturma

df["TotalPrice"] = df["Quantity"] * df["Price"]

##############################################################################################
# RFM Metriklerinin Hesaplanması
###############################################################################################

# Recency : Müşterinin son satın alma işleminden, analiz anına kadar geçen süreyi ifade eder.
# Yani müşterinin, şirket ile teması üzerinden geçen süredir. Formülü ise; Analiz Tarihi - Son Satın Alma Tarihi

# frequency : Müşterinin toplam satın alma sayısını ifade eder.(frequency>1)

# monetary : Müşterinin satın alma işlemlerinde toplam yaptığı harcamayı ifade eder.

# Müşteri özelinde R,F,M metriklerinin hesaplanması

df["InvoiceDate"].max() # 2011-12-09

today_date = dt.datetime(2011, 12, 11)

rfm = df.groupby("Customer ID").agg({"InvoiceDate": lambda InvoiceDate: (today_date - InvoiceDate.min()).days,
                                    "Invoice": lambda Invoice: Invoice.nunique(),
                                    "TotalPrice": lambda TotalPrice: TotalPrice.sum()})


rfm.columns = ["recency", "frequency", "monetary"]

rfm.describe().T

rfm = rfm[rfm["monetary"] > 0]

rfm.describe().T

##############################################################################################
# RFM Skorlarının Oluşturulması ve Tek bir Değişkene Çevrilmesi
###############################################################################################

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))

##############################################################################################
# RF Skorunun Segment Olarak Tanımlanması
###############################################################################################

# RFM isimlendirmesi

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)

##############################################################################################
# Aksiyon Zamanı !
###############################################################################################

# Segmentlerin RFM değerleri ortalamaları

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg("mean")

# Loyal_Customers sınıfana ait Customer Id leri seçerek excle çıktısı alalım

loyal_customers_df = pd.DataFrame()

loyal_customers_df["loyal_customers_id"] = rfm[rfm["segment"] == "loyal_customers"].index

loyal_customers_df.to_excel("loyal_customers_id.xlsx")

###############################################################
# Tüm Sürecin Fonksiyonlaştırılması
###############################################################

def create_rfm(dataframe, csv=False):

    #VERIYI HAZIRLAMA
    dataframe["TotalPrice"] = dataframe["Quantity"] * dataframe["Price"]
    dataframe.dropna(inplace=True)
    dataframe = dataframe[~dataframe["Invoice"].str.contains("C", na=False)]

    #RFM METRIKLERININ HESAPLANMASI
    today_date = dt.datetime(2011, 12, 11)
    rfm = dataframe.groupby("Customer ID").agg({"InvoiceDate": lambda date: (today_date - date.max()).days,
                                                "Invoice": lambda num: num.nunique(),
                                                "TotalPrice": lambda price: price.sum()})
    rfm.columns = ["recency", "frequency", "monetary"]
    rfm = rfm[rfm["monetary"] > 0]

    #RFM SkORLARININ HESAPLANMASI
    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[5, 4, 3, 2, 1])

    # cltv_df skorları kategorik degere donusturulup dfe kaydedildi
    rfm["RFM_SCORE"] = (rfm["recency_score"].astype(str) +
                        rfm["frequency_score"].astype(str))

    # SEGMENTLERIN ISIMLENDIRMESİ

    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    rfm["segment"] = rfm["RFM_SCORE"].replace(seg_map, regex=True)
    rfm = rfm[["recency", "frequency", "monetary", "segment"]]
    rfm.index = rfm.index.astype(int)

    if csv:
        rfm.to_csv("rfm.csv")

    return rfm

df = df_.copy()


rfm_new = create_rfm(df)
rfm_new


