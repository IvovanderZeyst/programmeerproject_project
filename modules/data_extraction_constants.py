ANALYSIS_TABLE_MAPPING = {
    'csv_read_options': {
        'separator': ',',
        'lineterminator': '\n',
        'quotechar': '"'
    },
    'id': {
        'rename_col_to': 'analysis_id',
        'data_type': 'string'
    },
    'model_id': {
        'rename_col_to': 'model_id',
        'data_type': 'string'
    },
    'specLevel_value': {
        'rename_col_to': 'spec_Level_value',
        'data_type': 'float'
    },
    'groupKey': {
        'rename_col_to': 'group_key',
        'data_type': 'string'
    },
    'frame': {
        'rename_col_to': 'frame',
        'data_type': 'float'
    },
    'wheels': {
        'rename_col_to': 'wheels',
        'data_type': 'float'
    },
    'brakes': {
        'rename_col_to': 'brakes',
        'data_type': 'float'
    },
    'groupset': {
        'rename_col_to': 'group_set',
        'data_type': 'float'
    },
    'shifting': {
        'rename_col_to': 'shifting',
        'data_type': 'float'
    },
    'seatpost': {
        'rename_col_to': 'seatpost',
        'data_type': 'float'
    },
    'forkMaterial': {
        'rename_col_to': 'fork_material',
        'data_type': 'float'
    },
    'forkrank': {
        'rename_col_to': 'forkrank',
        'data_type': 'float'
    },
    'valueProp': {
        'rename_col_to': 'value_prop',
        'data_type': 'float'
    }
}

MODELS_TABLE_MAPPING = {
    'csv_read_options': {
        'separator': ',',
        'lineterminator': '\n',
        'quotechar': '"'
    },
    'id': {
        'rename_col_to': 'id',
        'data_type': 'string'
    },
    'model_id': {
        'rename_col_to': 'model_id',
        'data_type': 'string'
    },
    'url': {
        'rename_col_to': 'url',
        'data_type': 'string'
    },
    'maker': {
        'rename_col_to': 'maker',
        'data_type': 'string'
    },
    'makerId': {
        'rename_col_to': 'maker_id',
        'data_type': 'string'
    },
    'year': {
        'rename_col_to': 'year',
        'data_type': 'integer'
    },
    'model': {
        'rename_col_to': 'model',
        'data_type': 'string'
    },
    'family': {
        'rename_col_to': 'family',
        'data_type': 'string'
    },
    'category': {
        'rename_col_to': 'category',
        'data_type': 'string'
    },
    'subcategory': {
        'rename_col_to': 'subcategory',
        'data_type': 'string'
    },
    'buildKind': {
        'rename_col_to': 'build_kind',
        'data_type': 'string'
    },
    'isFrameset': {
        'rename_col_to': 'is_frameset',
        'data_type': 'boolean'
    },
    'isEbike': {
        'rename_col_to': 'is_ebike',
        'data_type': 'boolean'
    },
    'gender': {
        'rename_col_to': 'gender',
        'data_type': 'string'
    }
}

PRICE_HISTORY_TABLE_MAPPING = {
    'csv_read_options': {
        'separator': ',',
        'lineterminator': '\n',
        'quotechar': '"'
    },
    'id': {
        'rename_col_to': 'price_history_id',
        'data_type': 'string'
    },
    'model_id': {
        'rename_col_to': 'model_id',
        'data_type': 'string'
    },
    'currency': {
        'rename_col_to': 'currency',
        'data_type': 'string'
    },
    'date': {
        'rename_col_to': 'date',
        'data_type': 'datetime',
        'datetime_format': '%Y-%m-%d'
    },
    'amount': {
        'rename_col_to': 'amount',
        'data_type': 'float'
    },
    'change': {
        'rename_col_to': 'change',
        'data_type': 'float'
    },
    'discountedAmount': {
        'rename_col_to': 'discounted_amount',
        'data_type': 'float'
    },
    'discount': {
        'rename_col_to': 'discount',
        'data_type': 'float'
    }
}


PRICES_TABLE_MAPPING = {
    'csv_read_options': {
        'separator': ',',
        'lineterminator': '\n',
        'quotechar': '"'
    },
    'id': {
        'rename_col_to': 'price_id',
        'data_type': 'string'
    },
    'model_id': {
        'rename_col_to': 'model_id',
        'data_type': 'string'
    },
    'currency': {
        'rename_col_to': 'currency',
        'data_type': 'string'
    },
    'amount': {
        'rename_col_to': 'amount',
        'data_type': 'float'
    },
    'discountedAmount': {
        'rename_col_to': 'discounted_amount',
        'data_type': 'float'
    },
    'discount': {
        'rename_col_to': 'discount',
        'data_type': 'float'
    }
}