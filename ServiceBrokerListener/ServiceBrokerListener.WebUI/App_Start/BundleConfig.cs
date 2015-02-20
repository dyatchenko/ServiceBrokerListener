namespace ServiceBrokerListener.WebUI.App_Start
{
    using System.Web.Optimization;

    public class BundleConfig
    {
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(
                new StyleBundle("~/Content/css").Include(
                    "~/Content/index.css",
                    "~/Content/handsontable.full.css"));
            bundles.Add(
                new StyleBundle("~/Content/Start/css").Include(
                    "~/Content/Start/*.css"));
            bundles.Add(
                new ScriptBundle("~/bundles/scripts").Include(
                    "~/Scripts/jquery-1.11.1.min.js",
                    "~/Scripts/jquery.signalR-2.2.0.min.js",
                    "~/Scripts/jquery.ui.core.min.js",
                    "~/Scripts/jquery.ui.widget.min.js",
                    "~/Scripts/jquery.ui.mouse.min.js",
                    "~/Scripts/jquery.ui.button.min.js",
                    "~/Scripts/jquery.ui.draggable.min.js",
                    "~/Scripts/jquery.ui.position.min.js",
                    "~/Scripts/jquery.ui.resizable.min.js",
                    "~/Scripts/jquery.ui.dialog.min.js",
                    "~/Scripts/jquery.ui.effect.min.js",
                    "~/Scripts/jquery.ui.effect-fade.min.js",
                    "~/Scripts/handsontable.full.js"));
        }
    }
}