"""Testing crawling the Daily Mail website and download article metadata/content."""

# Third party libraries
import requests

# Internal imports
from interlocutor.get_data import daily_mail


class MockColumnistHomepage:
    """Mock a stripped down version of the Daily Mail page which lists its columnists."""

    def __init__(self):
        self.status_code = 200
        self.content = b"""
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
            "//www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
        <html>
           <head>
           </head>
           <body class="tlc-columnists columnistscolumnists mol-desktop aileron-font" id="columnists"
                itemid="/columnists/index.html" itemscope="" itemtype="//schema.org/CollectionPage">
              <div class="debate item">
                 <div class="editors-choice ccox link-ccox linkro-darkred" id="p-6"
                    data-track-module="llg-1001345^editors_choice">
                    <h3 class="bdrcc">DAILY MAIL COLUMNISTS </h3>
                    <ul class="cleared">
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html">
                                BAZ BAMIGBOYE</a>
                       </li>
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html">
                                CRAIG BROWN</a>
                       </li>
                    </ul>
                 </div>
              </div>
              <script type="text/javascript">    DM.later(\'bundle\', function()
                {DM.has(\'p-6\', \'externalLinkTracker\');    });</script>
              <div class="debate item">
                 <div class="editors-choice ccox link-ccox linkro-darkred" id="p-45"
                 data-track-module="llg-1001682^editors_choice">
                    <h3 class="bdrcc">MAIL ON SUNDAY COLUMNISTS </h3>
                    <ul class="cleared">
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html">
                                PETER HITCHENS</a>
                       </li>
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl"
                                    href="/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html">
                                    LIZ JONES</a>
                       </li>
                    </ul>
                 </div>
              </div>
              <script type="text/javascript">    DM.later(\'bundle\', function()
                {DM.has(\'p-45\', \'externalLinkTracker\');    });</script>
              <div class="debate item">
                 <div class="editors-choice ccox link-ccox linkro-darkred" id="p-54"
                    data-track-module="llg-1000543^editors_choice">
                    <h3 class="bdrcc">RIGHTMINDS BLOGGERS </h3>
                    <ul class="cleared">
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="https://brummerblog.dailymail.co.uk/"
                                rel="nofollow noreferrer noopener" target="_blank">ALEX BRUMMER</a>
                       </li>
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="https://chapman.dailymail.co.uk/"
                                rel="nofollow noreferrer noopener" target="_blank">CHAPMAN & CO</a>
                       </li>
                    </ul>
                 </div>
              </div>
           </body>
        </html>
        """


def mock_columnist_homepage(url=None):
    """Mock the HTTP request to the page listing the paper's columnists."""

    return MockColumnistHomepage()


def test_get_columnist_homepages(monkeypatch):
    """The name of columnists is successfully retrieved alongside their homepage."""

    expected_columnists = {
        'Baz Bamigboye': '/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
        'Craig Brown': '/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
        'Peter Hitchens': '/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html',
        'Liz Jones': '/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
        'Alex Brummer': 'https://brummerblog.dailymail.co.uk/',
        'Chapman & Co': 'https://chapman.dailymail.co.uk/'
    }

    monkeypatch.setattr(requests, 'get', mock_columnist_homepage)

    actual_columnists = daily_mail.get_columnist_homepages()

    assert actual_columnists == expected_columnists
