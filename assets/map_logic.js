// On initialise les variables globales
var leafletMapInstance = null;
var selectionContour = null;

    function verrouillerBoutonsEdition(verrouiller) {
        var btnEdit = document.querySelector('.leaflet-draw-edit-edit');
        // var btnRemove = document.querySelector('.leaflet-draw-edit-remove');
        
        if (verrouiller) {
            // Désactive le mode édition s'il était actif
            if (leafletMapInstance && leafletMapInstance._toolbars && leafletMapInstance._toolbars.edit) {
                try { leafletMapInstance._toolbars.edit._modes.edit.handler.disable(); } catch(e){}
            }
            // Rend les boutons gris et incliquables
            if (btnEdit) { btnEdit.style.pointerEvents = 'none'; btnEdit.style.opacity = '0.4'; }
            // if (btnRemove) { btnRemove.style.pointerEvents = 'none'; btnRemove.style.opacity = '0.4'; }
        } else {
            // Réactive les boutons
            if (btnEdit) { btnEdit.style.pointerEvents = 'auto'; btnEdit.style.opacity = '1'; }
            // if (btnRemove) { btnRemove.style.pointerEvents = 'auto'; btnRemove.style.opacity = '1'; }
        }
    }

(function() {
    var pyHandlerIsReady = false;
    var leafletMapInstance = null;
    var lastDrawnRectangle = null;

    // FONCTION POUR DESSINER LE CONTOUR ET ZOOMER
    window.drawTerritory = function(featureData, showAsRectangle, shouldZoom) {
        if (!leafletMapInstance) {
            for (var k in window) { if (window[k] instanceof L.Map) { leafletMapInstance = window[k]; break; } }
        }
        
        if (leafletMapInstance) {
            // 1. Nettoyage de l'ancienne forme bleue ou pointillée
            if (window.selectionContour) { 
                leafletMapInstance.removeLayer(window.selectionContour); 
                window.selectionContour = null; 
            }
            
            // 2. Nettoyage VISUEL de l'ancien rectangle sur la carte
            if (lastDrawnRectangle) { 
                try { leafletMapInstance.removeLayer(lastDrawnRectangle); } catch(err){}
            }

            // 3. Vidage de la "boîte" des objets modifiables de l'outil de dessin
            if (leafletMapInstance._toolbars && leafletMapInstance._toolbars.edit) {
                leafletMapInstance._toolbars.edit.options.featureGroup.clearLayers();
            }
            
            lastDrawnRectangle = null;

            if (showAsRectangle) {
                // --- MODE RECTANGLE ---
                var tempLayer = L.geoJSON(featureData);
                var bounds = tempLayer.getBounds();

                // A. On affiche la commune en pointillé (non modifiable, en fond)
                window.selectionContour = L.geoJSON(featureData, {
                    style: { color: "#2c3e50", weight: 2, opacity: 0.6, fillOpacity: 0, dashArray: '5, 10' },
                    interactive: false
                }).addTo(leafletMapInstance);

                // B. On crée le rectangle
                var rect = L.rectangle(bounds, {
                    color: "#3498db", weight: 2, fillOpacity: 0.3, fillColor: "#3498db"
                });

                // LA RUSE EST ICI : On déclenche l'événement "draw:created" manuellement.
                // Cela fait croire à l'outil de dessin que l'utilisateur a tracé ce rectangle.
                // Ainsi, il l'ajoute à ses objets éditables et active le bouton "Edit".
                leafletMapInstance.fire('draw:created', { layer: rect, layerType: 'rectangle' });
                verrouillerBoutonsEdition(false);

                if (shouldZoom) { leafletMapInstance.fitBounds(bounds, {padding: [30, 30]}); }
                
            } else {
                // --- MODE PRÉCIS ---
                // Bleu fixe. Pas d'événement 'draw:created', donc le bouton Edit l'ignore.
                window.selectionContour = L.geoJSON(featureData, {
                    style: { color: "#2c3e50", weight: 3, opacity: 0.8, fillColor: "#3498db", fillOpacity: 0.2 },
                    interactive: false
                }).addTo(leafletMapInstance);
                verrouillerBoutonsEdition(true); // BLOQUE le bouton Edit
                
                if (shouldZoom) { leafletMapInstance.fitBounds(window.selectionContour.getBounds(), {padding: [30, 30]}); }
            }
        }
    };

    function findMapAndAttachDrawEvents() {
        if (!leafletMapInstance) {
            for (var k in window) { if (window[k] instanceof L.Map) { leafletMapInstance = window[k]; break; } }
        }
        if (leafletMapInstance && !leafletMapInstance._draw_events_attached) {
            
            // QUAND UN RECTANGLE EST DESSINÉ (A la main ou via notre ruse JS)
            leafletMapInstance.on('draw:created', function(e) {
                if (e.layerType === 'rectangle') {
                    if (lastDrawnRectangle) { try { leafletMapInstance.removeLayer(lastDrawnRectangle); } catch(err){} }
                    leafletMapInstance.addLayer(e.layer);
                    lastDrawnRectangle = e.layer;
                    
                    // Envoi à Python
                    if (window.pyHandler) { window.pyHandler.receive_bbox(JSON.stringify(e.layer.toGeoJSON())); }
                }
            });

            // --- NOUVEAU : QUAND LE RECTANGLE EST MODIFIÉ (VIA LE BOUTON EDIT) ---
            leafletMapInstance.on('draw:edited', function(e) {
                e.layers.eachLayer(function(layer) {
                    // On envoie les nouvelles coordonnées ajustées à Python
                    if (window.pyHandler) { window.pyHandler.receive_bbox(JSON.stringify(layer.toGeoJSON())); }
                });
            });

            // QUAND LE RECTANGLE EST SUPPRIMÉ
            leafletMapInstance.on('draw:deleted', function(e) {
                if (window.selectionContour) {
                    leafletMapInstance.removeLayer(window.selectionContour);
                    window.selectionContour = null;
                }
                lastDrawnRectangle = null;
                verrouillerBoutonsEdition(true); // RE-BLOQUE le bouton Edit car tout est vide
            });

            leafletMapInstance._draw_events_attached = true;
        }
    }

    function checkReadyState() {
        if (!pyHandlerIsReady && typeof qt !== 'undefined') {
            new QWebChannel(qt.webChannelTransport, function(channel) {
                window.pyHandler = channel.objects.pyHandler;
                pyHandlerIsReady = true;
            });
        }
        if (typeof L !== 'undefined' && L.map) { findMapAndAttachDrawEvents(); }
    }
    
    setInterval(checkReadyState, 500);
})();