package Koha::Plugin::Com::Library::Recommendations;

use Modern::Perl;
use base qw(Koha::Plugins::Base);
use C4::Context;         # Để lấy DB handle và userenv
use C4::Auth;
use JSON;                # Để encode/decode JSON

our $VERSION = "2.0";

our $metadata = {
    name            => 'Koha Recommender Plugin (Perl + Koha DB)',
    author          => 'Tran Tien',
    description     => 'Koha Recommender Plugin',
    date_authored   => '2025-04-12',
    date_updated    => '2025-05-03',
    minimum_version => '22.11',
    maximum_version => undef,
    version         => $VERSION,
};

sub new {
    my ( $class, $args ) = @_;
    $args->{'metadata'}            = $metadata;
    $args->{'metadata'}->{'class'} = $class;
    return $class->SUPER::new($args);
}

#----------------------------------------------------------------
# Lấy danh sách biblionumber từ virtualshelfcontents cho 1 user
#----------------------------------------------------------------
sub get_user_favorites_from_koha {
    my ($user_id) = @_;
    return [] unless $user_id;

    my $dbh = C4::Context->dbh;
    my $sql = q{
        SELECT biblionumber 
        FROM virtualshelfcontents 
        WHERE borrowernumber = ?
    };
    my $sth = $dbh->prepare($sql);
    $sth->execute($user_id);

    my @favorites;
    while ( my ($biblio) = $sth->fetchrow_array ) {
        push @favorites, $biblio;
    }
    return \@favorites;
}

#----------------------------------------------------
# Chèn JavaScript hiển thị gợi ý lên giao diện OPAC
#----------------------------------------------------
sub opac_js {
    my ( $self, $args ) = @_;

    # 1. Lấy user_id từ session Koha
    my $user_id = "";
    if ( C4::Context->userenv and C4::Context->userenv->{'number'} ) {
        $user_id = C4::Context->userenv->{'number'};
    }

    # 2. Lấy danh sách favorites
    my $favorites_ref = get_user_favorites_from_koha($user_id);

    # 3. Chuyển sang JSON để nhúng vào JS
    my $favorites_json = to_json($favorites_ref);

    # 4. Sinh đoạn mã JavaScript
    my $recommender_js = << "END_JS";

<script>
document.addEventListener('DOMContentLoaded', function() {
    var userId    = "$user_id";
    var favorites = $favorites_json;

    // dùng itemnumber, để Koha tự tìm biblionumber tương ứng:
    var basePath = window.location.pathname.split("/cgi-bin/")[0];
    var detail_base = window.location.origin + basePath + "/cgi-bin/koha/opac-MARCdetail.pl?biblionumber=";


    function fetchRecommendations() {
        fetch('http://172.22.24.223:8085/get_recommendations', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: parseInt(userId), topN: 10, favorites: favorites })
        })
        .then(function(res) {
            if (!res.ok) throw new Error('HTTP status ' + res.status);
            return res.json();
        })
        .then(function(data) {
            // 1. Tạo hoặc lấy lại container
            var container = document.getElementById("recommender-container");
            if (!container) {
                container = document.createElement("div");
                container.id = "recommender-container";
                container.style.width           = "100%";
                container.style.backgroundColor = "#fff";
                container.style.padding         = "8px 16px";
                container.style.boxSizing       = "border-box";
                container.style.marginBottom    = "4px";    // gap nhỏ với banner
                container.style.borderRadius    = "4px";

                // Header
                var header = document.createElement("h3");
                header.textContent = "Recommended for you";
                header.style.margin   = "0 0 8px";
                header.style.fontSize = "16px";
                header.style.color    = "#003366";
                container.appendChild(header);

                // Wrapper cho cards
                var cardsWrapper = document.createElement("div");
                cardsWrapper.id = "recommender-cards-wrapper";
                cardsWrapper.style.overflow   = "hidden";
                cardsWrapper.style.whiteSpace = "nowrap";
                cardsWrapper.style.height     = "60px";
                container.appendChild(cardsWrapper);
                
                var styleEl = document.createElement('style');
                // CSS single-line để tránh lỗi token
                styleEl.textContent = "#recommender-cards-wrapper > div { scrollbar-width: none; -ms-overflow-style: none; } #recommender-cards-wrapper > div::-webkit-scrollbar { display: none; }";
                document.head.appendChild(styleEl);

                // ==== ƯU TIÊN CHÈN SAU BREADCRUMB TRANG YOUR SUMMARY ====
                // 1. Đầu tiên tìm <ol class="breadcrumb"> hoặc <ul class="breadcrumb">
                var breadcrumb = document.querySelector('ol.breadcrumb, ul.breadcrumb');
                if (breadcrumb && breadcrumb.parentNode) {
                    // chèn ngay sau breadcrumb
                    breadcrumb.parentNode.insertBefore(container, breadcrumb.nextSibling);
                }
                // 2. Nếu không có breadcrumb (ví dụ OPAC khác), fallback về navBar cũ
                else {
                    var navBar = document.getElementById("containerColor")
                            || document.querySelector("nav")
                            || document.querySelector(".navbar");
                    if (navBar && navBar.parentNode) {
                        navBar.parentNode.insertBefore(container, navBar.nextSibling);
                    }
                    // 3. Cuối cùng fallback về body
                    else {
                        document.body.insertBefore(container, document.body.firstChild);
                    }
                }
            }

            // 2. Fill dữ liệu vào cards-wrapper
            var wrap = document.getElementById("recommender-cards-wrapper");
            wrap.innerHTML = "";
            var strip = document.createElement("div");
            strip.style.display    = "inline-block";
            strip.style.whiteSpace = "nowrap";

            data.forEach(function(item, idx) {
                var card = document.createElement("div");
                card.style.display      = "inline-block";
                card.style.verticalAlign= "top";
                card.style.width        = "220px";
                card.style.height       = "60px";
                card.style.marginRight  = (idx === data.length - 1 ? "30px" : "10px");
                card.style.borderRadius = "4px";
                card.style.backgroundColor = "#E8F5E9";   // xanh lá pastel
                card.style.backgroundSize   = "cover";
                card.style.transition    = "transform 0.2s";
                card.onmouseover        = function() { this.style.transform = "scale(1.05)"; };
                card.onmouseout         = function() { this.style.transform = "scale(1)"; };
                // --- CHO PHÉP SCROLL VERTICAL TRONG CARD ---
                card.style.overflowY    = "auto";
                card.style.overflowX    = "hidden";  
                card.style.boxSizing    = "border-box";

                // ====== BẮT ĐẦU PHẦN MÌNH CHÈN THÊM ======
                // Cho phép pointer-hiệu ứng và gắn sự kiện click redirect
                card.style.cursor = "pointer";
                card.addEventListener('click', function() {
                    var itemNo = item.item_id;          // Dùng đúng key JSON
                    if (itemNo) {
                        // Redirect sang opac-detail.pl?itemnumber=<itemNo>
                        window.location.href = detail_base + itemNo;
                    }
                });
                // ====== KẾT THÚC PHẦN MÌNH CHÈN THÊM ======

                var title = document.createElement("div");
                title.textContent        = item.title;
                title.style.display      = "block";
                title.style.padding      = "8px";
                title.style.fontSize     = "12px";
                title.style.fontWeight   = "bold";
                title.style.color        = "#333";
                title.style.overflow     = "visible";
                title.style.lineHeight   = "1.2em";
                title.style.margin       = "0";
                title.style.padding      = "4px";
                title.style.textOverflow = "clip";
                title.style.whiteSpace   = "normal"; // cho phép wrap

                card.appendChild(title);
                strip.appendChild(card);
            });
            wrap.appendChild(strip);

            // 3. Auto-scroll
            var scrollSpeed  = 1;
            var intervalTime = 30;
            setInterval(function() {
                wrap.scrollLeft += scrollSpeed;
                if (wrap.scrollLeft >= (strip.scrollWidth - wrap.clientWidth - 30)) {
                    wrap.scrollLeft = 0;
                }
            }, intervalTime);

        })
        .catch(function(err) {
            console.error("Recommender error:", err);
        });
    }

    // Gọi API lần đầu
    fetchRecommendations();

    // Khi favorites được cập nhật
    document.addEventListener("favoriteUpdated", function(e) {
        if (e.detail && e.detail.newFavorite && favorites.indexOf(e.detail.newFavorite) < 0) {
            favorites.push(e.detail.newFavorite);
            fetchRecommendations();
        }
    });
});
</script>
END_JS

    return $recommender_js;
}

1;
