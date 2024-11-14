
import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset as TorchDataset

def prepare_x(data, lv):
    lv = int(lv/2)
    # 첫 번째 범위: 30-lv부터 30+lv까지
    range1 = data.iloc[:, 30-lv : 30+lv]

    df1 = data.iloc[:, 30-lv : 30+lv]
    range2 = data.iloc[:, 61:-1]

    # 두 범위를 합쳐 새로운 df1 생성
    df1 = pd.concat([range1, range2], axis=1)
    return np.array(df1)

def get_target(data):
    return np.array(data.iloc[:, -1])

# T : look back
def data_classification(X, Y, T):
    # N : 행 개수 D : 열 개수
    [N, D] = X.shape

    df = np.array(X)
    dY = np.array(Y)

    dataY = dY[T - 1:N]
    # T-1 ~ N 사이 개수 만큼의 데이터, T,
    dataX = np.zeros((N - T + 1, T, D))

    for i in range(T, N + 1):
        dataX[i - T] = df[i - T:i, :]
        # Min-max 정규화를 위해 각 열의 최소값과 최대값 계산
        min_val = dataX[i - T].min(axis=0)
        max_val = dataX[i - T].max(axis=0)
        # 0으로 나누는 것을 방지하기 위해 조건 추가
        range_val = max_val - min_val
        range_val[range_val == 0] = 1
        # 세로 방향으로 정규화
        dataX[i - T] = (dataX[i - T] - min_val) / range_val

    return dataX, dataY

def get_index(data, start_time_str, end_time_str, remove_targets):
    data['window_start'] = pd.to_datetime(data['window_start'], errors='coerce')
    time = data['window_start'].dt.time

    start_time = pd.to_datetime(start_time_str).time()
    end_time = pd.to_datetime(end_time_str).time()
    
    index_1 = data[(time >= start_time) & (time <= end_time)].index.tolist()
    
    remove_condition = pd.Series([False] * len(data), index=data.index)
    
    # Loop over each target and update the remove_condition mask
    for target in remove_targets:
        if target is None:  # If the target is None, look for NaN values
            remove_condition |= data.isnull().any(axis=1)
        else:
            # Use applymap to create a DataFrame of booleans where the condition matches
            # then use any(axis=1) to reduce it to a Series of booleans per row
            remove_condition |= data.applymap(lambda x: x == target).any(axis=1)
            
    index_2 = np.where(remove_condition)[0]
    
    for i in index_2:
        index_1.append(i)
        
    index_1 = set(index_1)
    
    return index_1

def expand_indices_correctly(indices, T, length):
    expanded = set(indices)  # indices 집합의 모든 요소를 먼저 expanded에 추가

    for index in indices:
        for i in range(index-T, index+T+1):  # index+T까지 포함하기 위해 +1을 함
            if 0 <= i < length:  # i가 0 이상이고 length 미만일 때만 추가
                expanded.add(i)
    return list(expanded)


def remove_data(x, y, original_data, remove_times, remove_targets, T):
    indices_to_remove = set()

    for start_time, end_time in remove_times :
        indices = get_index(original_data, start_time, end_time, remove_targets)
        print(indices)
        indices_to_remove.update(indices)

    # Calculate their expanded set
    length = original_data.shape[0] - T # x.shape[0] 같은 지 확인
    indices_to_remove = list(indices_to_remove)
    indices_to_remove = [x - T for x in indices_to_remove]
    print(indices_to_remove)
    expanded_indices = expand_indices_correctly(indices_to_remove, T, length)
    print(expanded_indices)

    # Remove the sequences that fall within these expanded indices
    x = np.delete(x, expanded_indices, axis=0)
    y = np.delete(y, expanded_indices, axis=0)

    return x, y



class Dataset(data.Dataset):
    """Characterizes a dataset for PyTorch"""
    def __init__(self, data, T, lv, remove_times, remove_targets):
        """Initialization"""
        self.T = T

        x = prepare_x(data, lv)
        y = get_target(data)

        x, y = data_classification(x, y, self.T)


        # remove drawn

        if remove_times:
            x, y = remove_data(x, y, data, remove_times,remove_targets, self.T)
        self.length = len(x)

        x = torch.from_numpy(x)
        self.x = torch.unsqueeze(x, 1)
        self.y = torch.from_numpy(y)

    def __len__(self):
        """Denotes the total number of samples"""
        return self.length

    def __getitem__(self, index):
        """Generates samples of data"""
        return self.x[index], self.y[index]
