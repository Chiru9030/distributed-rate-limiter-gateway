(function () {
  function parsePrometheus(text) {
    var map = {};
    text.split('\n').forEach(function (line) {
      if (!line || line.charAt(0) === '#') return;
      var parts = line.trim().split(/\s+/);
      if (parts.length >= 2) map[parts[0]] = Number(parts[1]);
    });
    return map;
  }

  function adminHeaders() {
    var key = $('#adminKeyInput').val().trim();
    return key ? { 'X-API-Key': key } : {};
  }

  function loadTelemetry() {
    $.ajax({ url: '/metrics', method: 'GET', dataType: 'text' })
      .done(function (body) {
        var m = parsePrometheus(body);
        $('#kpiTotal').text(m.gateway_requests_total || 0);
        $('#kpiRateLimited').text(m.gateway_rate_limited_total || 0);
        $('#kpiAuthErr').text(m.gateway_auth_failures_total || 0);
        $('#kpiUpErr').text(m.gateway_upstream_errors_total || 0);
      })
      .fail(function (xhr) {
        $('#statusNotice').text('Unable to fetch /metrics (HTTP ' + xhr.status + ').');
      });

    $.ajax({ url: '/v1/admin/status', method: 'GET', headers: adminHeaders() })
      .done(function (data) {
        $('#statusNotice').text('Gateway control plane synchronized (' + data.routes.length + ' route pool(s)).');
        var tbody = $('#routesBody').empty();
        data.routes.forEach(function (route) {
          route.nodes.forEach(function (n) {
            var tr = $('<tr></tr>');
            tr.append($('<td></td>').text(route.path_prefix));
            tr.append($('<td></td>').text(route.strategy || 'round_robin'));
            tr.append($('<td></td>').text(n.url));
            tr.append($('<td></td>').text(String(n.weight || 1)));
            var badge = $('<span></span>')
              .addClass('badge ' + (n.healthy ? 'ok' : 'err'))
              .text(n.healthy ? 'HEALTHY' : 'UNHEALTHY');
            tr.append($('<td></td>').append(badge));
            tbody.append(tr);
          });
        });
      })
      .fail(function (xhr) {
        $('#statusNotice').text('Control plane status requires valid Admin API Key when enforced (HTTP ' + xhr.status + ').');
      });
  }

  $('#refreshBtn').on('click', loadTelemetry);
  $('#adminKeyInput').on('change', loadTelemetry);

  $('#provisionBtn').on('click', function () {
    var payload = {
      APIKey: $('#tenantKey').val().trim(),
      Tenant: $('#tenantName').val().trim(),
      Tier: 'pro',
      Policy: {
        Algorithm: $('#tenantAlgo').val(),
        Limit: Number($('#tenantLimit').val() || 10),
        Window: 60000000000,
        RefillRate: Number($('#tenantRefill').val() || 2)
      }
    };
    $.ajax({
      url: '/v1/admin/tenants',
      method: 'POST',
      contentType: 'application/json',
      headers: adminHeaders(),
      data: JSON.stringify(payload)
    }).done(function (res) {
      $('#provisionOut').text(JSON.stringify(res, null, 2));
      if (payload.APIKey) $('#probeKey').val(payload.APIKey);
      loadTelemetry();
    }).fail(function (xhr) {
      $('#provisionOut').text('HTTP ' + xhr.status + ': ' + (xhr.responseText || 'Request failed'));
    });
  });

  $('#probeBtn').on('click', function () {
    var path = $('#probePath').val().trim() || '/v1/status';
    var key = $('#probeKey').val().trim();
    $.ajax({
      url: path,
      method: 'GET',
      headers: key ? { 'X-API-Key': key } : {}
    }).done(function (res, _status, xhr) {
      var meta = 'HTTP ' + xhr.status + ' | Remaining: ' + xhr.getResponseHeader('X-RateLimit-Remaining') + '\n';
      $('#probeOut').text(meta + (typeof res === 'string' ? res : JSON.stringify(res, null, 2)));
      loadTelemetry();
    }).fail(function (xhr) {
      var meta = 'HTTP ' + xhr.status + ' | Retry-After: ' + (xhr.getResponseHeader('Retry-After') || '-') + '\n';
      $('#probeOut').text(meta + (xhr.responseText || 'Error'));
      loadTelemetry();
    });
  });

  $(document).ready(loadTelemetry);
})();
