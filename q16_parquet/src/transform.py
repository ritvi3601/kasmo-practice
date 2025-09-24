import pandas as pd
def transform(df1,df2):
    #df1['mainroad']=df1['mainroad'].str.lower().map({'yes':True,'no':False})
    df1['guestroom']=df1['guestroom'].str.lower().map({'yes':True,'no':False})
    df1['basement']=df1['basement'].str.lower().map({'yes':True,'no':False})
    df1['hotwaterheating']=df1['hotwaterheating'].str.lower().map({'yes':True,'no':False})
    df1['airconditioning']=df1['airconditioning'].str.lower().map({'yes':True,'no':False})
    df1['prefarea']=df1['prefarea'].str.lower().map({'yes':True,'no':False})

    return df1,df2