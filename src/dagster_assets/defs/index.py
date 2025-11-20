
import dagster as dg
from dagster_assets.utils import log_and_run
from dagster_assets.defs.estc import estc_overview
from dagster_assets.defs.cnb import cnb_overview
from dagster_assets.defs.dnb import dnb_overview
from dagster_assets.defs.erb import erb_overview
from dagster_assets.defs.gnd import gnd_overview
from dagster_assets.defs.fennica import fennica_overview
from dagster_assets.defs.hpb import hpb_overview
from dagster_assets.defs.istc import istc_overview
from dagster_assets.defs.plnb import plnb_overview
from dagster_assets.defs.ptnb import ptnb_overview
from dagster_assets.defs.vd17 import vd17_overview
from dagster_assets.defs.vd18 import vd18_overview
from dagster_assets.defs.viaf import viaf_overview
from dagster_assets.defs.bnf import bnf_overview
from dagster_assets.defs.kbnl import kbnl_overview
from dagster_assets.defs.kbse import kbse_overview
from dagster_assets.defs.stcn import stcn_overview
from dagster_assets.defs.stcv import stcv_overview
from dagster_assets.defs.idloc import idloc_overview
from dagster_assets.defs.ulan import ulan_overview
from dagster_assets.defs.tgn import tgn_overview
from dagster_assets.defs.melinda import melinda_overview
from dagster_assets.defs.dbnf import dbnf_overview
from dagster_assets.defs.isni import isni_overview
from dagster_assets.defs.wikidata import wikidata_overview
from dagster_assets.defs.geonames import geonames_overview

@dg.asset(deps=[isni_overview, wikidata_overview, geonames_overview, idloc_overview, ulan_overview, tgn_overview, melinda_overview, dbnf_overview, estc_overview, cnb_overview, stcn_overview, stcv_overview, dnb_overview, erb_overview, gnd_overview, fennica_overview, hpb_overview, istc_overview, plnb_overview, ptnb_overview, vd17_overview, vd18_overview, viaf_overview, bnf_overview, kbnl_overview, kbse_overview])
def index(context: dg.AssetExecutionContext):
    cmd = "python src/create-index.py"
    log_and_run(cmd, context)

