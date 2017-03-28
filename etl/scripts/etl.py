# -*- coding: utf-8 -*-

import os
import re
import shutil
import pandas as pd
from ddf_utils.index import get_datapackage

# paths
source_path = '../source'
out_path = '../../'

# get all indicators
indicators = set()

for f in os.listdir(source_path):
    if re.match('ddf--datapoints--[a-z0-9_]*--.*', f):
        g = re.match('ddf--datapoints--([a-z0-9_]*)--.*', f).groups()[0]
        if g.endswith('_male') or g.endswith('_female'):
            continue
        indicators.add(g)


def datapoints_by_basomrade_gender():
    """create datapoints by basomrade/gender/year and copy datapoints for
    basomrade/year
    """
    res = list()
    for indicator in indicators:

        if indicator == 'indicators':
            continue

        ilist = [
            indicator,
            indicator+'_male',
            indicator+'_female'
        ]

        data = []

        for i in ilist:
            d = pd.read_csv(os.path.join(source_path,
                                         'ddf--datapoints--{}--by--basomrade--year.csv'.format(i)))
            d = d.rename(columns={i: indicator})
            if '_female' in i:
                d['gender'] = 'female'
            elif '_male' in i:
                d['gender'] = 'male'
            else:
                shutil.copy(os.path.join(source_path,
                                         'ddf--datapoints--{}--by--basomrade--year.csv'.format(i)),
                            out_path)
                continue
            data.append(d)

        finaldata = pd.concat(data, ignore_index=True)
        res.append(finaldata)
        finaldata.to_csv(
            os.path.join(out_path,
                         'ddf--datapoints--{}--by--gender--basomrade--year.csv'.format(indicator)),
            index=False
        )
    return res


def datapoints_by_municipality():
    """create a file contains all indicators by municipality/year"""
    path = os.path.join(source_path,
                        'ddf--datapoints--indicators--by--municipality--year.csv')
    dm = pd.read_csv(path)
    dm = dm.set_index(['municipality', 'year'])
    dm_data = dict()

    for name, ser in dm.iteritems():
        if name.endswith('_male'):
            gend = 'male'
            d = ser.copy()
            d.name = name.replace('_male', '')
        elif name.endswith('_female'):
            gend = 'female'
            d = ser.copy()
            d.name = name.replace('_female', '')
        else:
            continue
            gend = 'both'
            d = ser.copy()

        n = d.name
        d = d.reset_index()
        d['gender'] = gend

        if n in dm_data.keys():
            dm_data[n].append(d.set_index(['gender', 'municipality', 'year']))
        else:
            dm_data[n] = [d.set_index(['gender', 'municipality', 'year'])]

    dm_comp_0 = [pd.concat(x) for x in dm_data.values()]
    dm_comp = pd.concat(dm_comp_0, axis=1)

    dm_comp.to_csv(
        os.path.join(out_path,
                     'ddf--datapoints--indicators--by--gender--municipality--year.csv'))
    dm[dm_comp.columns].to_csv(
        os.path.join(out_path, 'ddf--datapoints--indicators--by--municipality--year.csv'))


def entity_gender():
    gender = pd.DataFrame([['male', 'Male'], ['female', 'Female']],
                          columns=['gender', 'name'])
    gender.to_csv(os.path.join(out_path, 'ddf--entities--gender.csv'), index=False)


def copy_other_files():
    for f in os.listdir(source_path):
        if 'entities' in f or 'concepts' in f or f == 'datapackage.json':
            shutil.copy(os.path.join(source_path, f), out_path)


def apply_path_concepts():
    from ddf_utils.patch import apply_patch

    new_concepts = apply_patch(
        os.path.join(source_path, 'ddf--concepts.csv'),
        './concept_patch.csv')
    new_concepts.to_csv(os.path.join(out_path, 'ddf--concepts.csv'), index=False)


if __name__ == '__main__':
    datapoints_by_basomrade_gender()
    datapoints_by_municipality()
    entity_gender()
    copy_other_files()
    apply_path_concepts()
    get_datapackage(out_path, to_disk=True, use_existing=True)

    print('Done.')
    print('Please manually edit the entries in datapackage.json for '
          'ddf--datapoints--indicators--by--gender--municipality--year.csv and '
          'ddf--datapoints--indicators--by--gender--municipality--year.csv')
