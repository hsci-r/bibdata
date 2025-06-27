
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

@dg.asset(deps=[estc_overview, cnb_overview, dnb_overview, erb_overview, gnd_overview, fennica_overview, hpb_overview, istc_overview, plnb_overview, ptnb_overview, vd17_overview, vd18_overview, viaf_overview])
def index(context: dg.AssetExecutionContext):
    cmd = "python src/create-index.py"
    log_and_run(cmd, context)

