import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump
import logging

from contants import CLEAN_DATA_DIRECTORY

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


def train_and_save_model(model, X, y, path_to_model=f'{CLEAN_DATA_DIRECTORY}/model.pckl'):
    # training the model
    model.fit(X, y)
    
    # saving model
    logging.info(f"{str(model)} is saved at location: {path_to_model}")
    dump(model, path_to_model)


def prepare_data(path_to_data=f'{CLEAN_DATA_DIRECTORY}/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def create_model(kind:str):
    """
    Gets score model using cross validation for the given ML model kind.
    available kinds are:
    - `lr` => `LinearRegression`
    - `dt` => `DecisionTreeRegressor`
    - `rf` => `RandomForestRegressor`
    """
    model = None
    if kind == 'lr':
        model = LinearRegression()
    elif  kind == 'dt':
        model = DecisionTreeRegressor()
    elif kind == 'rf':
        model = DecisionTreeRegressor()
    else:
        raise KeyError('Type {type} is not handled!')

    return model

def get_score_model(kind:str):
    model = create_model(kind=kind)
    X, y = prepare_data(f'{CLEAN_DATA_DIRECTORY}/fulldata.csv')

    score = compute_model_score(model=model, X=X, y=y)
    logging.info(f'Model of kind {kind} has a score={score}')
    return score

def train_model(kind:str):
    model = create_model(kind=kind)
    X, y = prepare_data(f'{CLEAN_DATA_DIRECTORY}/fulldata.csv')

    train_and_save_model(model=model, X=X, y=y)