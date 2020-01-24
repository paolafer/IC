import pytest
import numpy  as np
import tables as tb
import pandas as pd

from .. core.configure import configure
from .. core.testing_utils import assert_dataframes_equal
from . paola import paola

files_in = '/Users/paola/Software/IC-crash-course/data/hdst.h5'
file_out = '/Users/paola/Software/IC-crash-course/data/paola_test.h5'
sel_event_file = '/Users/paola/Software/IC-crash-course/data/events.txt'
evts_to_sel = np.loadtxt(sel_event_file, dtype=int)


def test_selection_is_correct():

    conf = configure('dummy invisible_cities/config/paola.conf'.split())
    conf.update(dict(file_out = file_out))
    paola(**conf)

    with tb.open_file(file_out) as h5out:
        selected_evts = h5out.root.Run.events[:]['evt_number']

        assert np.all(evts_to_sel == selected_evts)


def test_content_is_the_same():

    conf = configure('dummy invisible_cities/config/paola.conf'.split())
    conf.update(dict(file_out = file_out))
    paola(**conf)

    with tb.open_file(files_in) as h5in:

        in_table = getattr(getattr(h5in.root, 'RECO'), 'Events').read()
        in_hits = pd.DataFrame.from_records(in_table)
        sel_in_hits = in_hits[in_hits.event.isin(evts_to_sel)].reset_index(drop=True)

        in_table = getattr(getattr(h5in.root, 'DST'), 'Events').read()
        in_kdsts = pd.DataFrame.from_records(in_table)
        sel_in_kdsts = in_kdsts[in_kdsts.event.isin(evts_to_sel)].reset_index(drop=True)

        in_table = getattr(getattr(h5in.root, 'Run'), 'events').read()
        in_times = pd.DataFrame.from_records(in_table)
        sel_in_times = in_times[in_times.evt_number.isin(evts_to_sel)].reset_index(drop=True)
        
        with tb.open_file(file_out) as h5out:

            out_table = getattr(getattr(h5out.root, 'RECO'), 'Events').read()
            out_hits = pd.DataFrame.from_records(out_table).reset_index(drop=True)

            out_table = getattr(getattr(h5out.root, 'DST'), 'Events').read()
            out_kdsts = pd.DataFrame.from_records(out_table).reset_index(drop=True)

            out_table = getattr(getattr(h5out.root, 'Run'), 'events').read()
            out_times = pd.DataFrame.from_records(out_table).reset_index(drop=True)
            
            assert_dataframes_equal(sel_in_hits, out_hits)
            assert_dataframes_equal(sel_in_kdsts, out_kdsts)
            assert_dataframes_equal(sel_in_times, out_times)

    
#def test_output_has_same_table_as_input():
    
#    conf = configure('invisible_cities/config/paola.conf')
#    res = paola(**configure(args))

#    with tb.open_file(file_in) as h5in:
#        with tb.open_file(file_out) as h5out:
#            for group_in in h5in.walk_groups("/"):
#                nodes_in = h5in.list_nodes(group_in)
#                for group_out in h5out.walk_groups("/"):
#                    nodes_out = h5out.list_nodes(group_out)
#                    assert nodes_in = nodes_out
            
               # table = getattr(getattr(h5in.root, 'RECO'), 'Events').read()    
